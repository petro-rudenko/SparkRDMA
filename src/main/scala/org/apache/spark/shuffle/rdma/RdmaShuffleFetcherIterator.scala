/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.shuffle.rdma

import java.io.InputStream
import java.nio.ByteBuffer
import java.util.Comparator
import java.util.concurrent.{LinkedBlockingQueue, PriorityBlockingQueue, ThreadLocalRandom}
import java.util.concurrent.atomic.{AtomicInteger, AtomicLong}

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.{SparkEnv, TaskContext}
import org.apache.spark.internal.Logging
import org.apache.spark.network.buffer.ManagedBuffer
import org.apache.spark.shuffle.{FetchFailedException, MetadataFetchFailedException}
import org.apache.spark.storage.{BlockId, BlockManagerId, ShuffleBlockId}
import org.apache.spark.util.Utils


private[spark] final class RdmaShuffleFetcherIterator(
    context: TaskContext,
    startPartition: Int,
    endPartition: Int,
    shuffleId : Int,
    blocksByAddress: Seq[(BlockManagerId, Seq[(BlockId, Long)])])
  extends Iterator[InputStream] with Logging {
  import org.apache.spark.shuffle.rdma.RdmaShuffleFetcherIterator._

  // numBlocksToFetch is initialized with "1" so hasNext() will return true until all of the remote
  // fetches has been started. The remaining extra "1" will be fulfilled with a null InputStream in
  // insertDummyResult()

  private[this] var numBlocksProcessed = 0

  private[this] val rdmaShuffleManager =
    SparkEnv.get.shuffleManager.asInstanceOf[RdmaShuffleManager]

  private[this] val resultsQueue = new LinkedBlockingQueue[FetchResult]

  @volatile private[this] var currentResult: FetchResult = _

  private[this] val shuffleMetrics = context.taskMetrics().createTempShuffleReadMetrics()

  @volatile private[this] var isStopped = false

  private[this] val localBlockManagerId = SparkEnv.get.blockManager.blockManagerId
  private[this] val rdmaShuffleConf = rdmaShuffleManager.rdmaShuffleConf

  private[this] val curBytesInFlight = new AtomicLong(0)

  case class PendingFetch(blockManagerId: BlockManagerId,
                          blocksTofetch: Seq[MapIdReduceId],
                          totalLength: Long)

  private val groupedBlocksByAddress = blocksByAddress.filter(_._1 != localBlockManagerId).map {
      case (blockManagerId, seq) => (blockManagerId, seq.filter(_._2 > 0))
    }.filter(_._2.nonEmpty)
  // Make random ordering of pending fetches to prevent oversubscription to channel
  val rand = ThreadLocalRandom.current()
  private[this] val pendingFetchesQueue = new PriorityBlockingQueue[PendingFetch](100,
    new Comparator[PendingFetch] {
      override def compare(o1: PendingFetch, o2: PendingFetch): Int = -1 + rand.nextInt(3)
    })

  private[this] val rdmaShuffleReaderStats = rdmaShuffleManager.rdmaShuffleReaderStats

  private[this] val numBlocksToFetch = new AtomicInteger(groupedBlocksByAddress.map(_._2.size).sum)
  logInfo(s"Starting reduceId: ${context.partitionId()}, num remote blocks to fetch: " +
    s"${numBlocksToFetch.get()}")

  initialize()

  private[this] def cleanup() {
    logDebug(s"ShuffleId: $shuffleId, reduceId: ${context.partitionId()}\n" +
            s"Number blocks processed: $numBlocksProcessed,\n" +
            s"total fetch wait time: ${shuffleMetrics.fetchWaitTime}")
    isStopped = true
    currentResult match {
      case SuccessFetchResult(_, _, inputStream) if inputStream != null => inputStream.close()
      case _ =>
    }
    currentResult = null

    val iter = resultsQueue.iterator()
    while (iter.hasNext) {
      val result = iter.next()
      result match {
        case SuccessFetchResult(_, blockManagerId, inputStream) if inputStream != null =>
          if (blockManagerId != localBlockManagerId) {
            shuffleMetrics.incRemoteBytesRead(inputStream.available)
            shuffleMetrics.incRemoteBlocksFetched(1)
          }
          inputStream.close()
        case _ =>
      }
    }
  }


  private[this] def splitPendingFetches(groupedBlocksByAddress:
                                        Seq[(BlockManagerId, Seq[(BlockId, Long)])]): Unit = {
    for ((blockManagerId, blocks) <- groupedBlocksByAddress) {
      var totalLength = 0L
      var blocksToFecth = new ArrayBuffer[MapIdReduceId]
      val allowedMsgSize = rdmaShuffleConf.recvWrSize - 32 -
        RdmaShuffleManagerId.serializedLength(blockManagerId)
      for (block <- blocks) {
        if (totalLength >= rdmaShuffleConf.shuffleReadBlockSize ||
          (blocksToFecth.size + 1) * 12 >= allowedMsgSize) {
          val pendingFetch = PendingFetch(blockManagerId, blocksToFecth, totalLength)
          pendingFetchesQueue.add(pendingFetch)
          blocksToFecth = new ArrayBuffer[MapIdReduceId]
          totalLength = 0L
        }
        val shuffleBlockId = block._1.asInstanceOf[ShuffleBlockId]
        blocksToFecth += MapIdReduceId(shuffleBlockId.mapId, shuffleBlockId.reduceId)
        totalLength += block._2
      }
      if (!blocksToFecth.isEmpty) {
        val pendingFetch = PendingFetch(blockManagerId, blocksToFecth, totalLength)
        pendingFetchesQueue.add(pendingFetch)
      }
    }
  }

  private[this] def sendPrefetchMsg(): Unit = {
    val pendingFetch = pendingFetchesQueue.poll()
    val channel = rdmaShuffleManager.getRdmaChannel(
      rdmaShuffleManager.blockManagerIdToRdmaShuffleManagerId(pendingFetch.blockManagerId), true)
    if (pendingFetch == null) return
    curBytesInFlight.addAndGet(pendingFetch.totalLength)
    val blocks = pendingFetch.blocksTofetch
    val totalBufferSize = (pendingFetch.totalLength.toInt * 2.5).toInt
    val TOTAL_BUFFER_SIZE = totalBufferSize + 4 * blocks.size
    val rdmaRegisteredBuffer: RdmaRegisteredBuffer =
      rdmaShuffleManager.getRdmaRegisteredBuffer(TOTAL_BUFFER_SIZE)
    val startRemoteFetchTime = System.currentTimeMillis()
    val blocksSize = blocks.size

    val rdmaWriteListener: RdmaCompletionListener = new RdmaCompletionListener {
      override def onSuccess(paramBuf: ByteBuffer): Unit = {
        curBytesInFlight.addAndGet(-pendingFetch.totalLength)
        val actualBlockLengths = rdmaRegisteredBuffer.getByteBuffer(4 * blocksSize)
        val rdmaByteBufferManagedBuffers = (0 until blocksSize).map(_ => {
            val actualBlockSize = actualBlockLengths.getInt()
            new RdmaByteBufferManagedBuffer(rdmaRegisteredBuffer, actualBlockSize)
          })

        rdmaByteBufferManagedBuffers.foreach { buf =>
          if (!isStopped) {
            val inputStream = new BufferReleasingInputStream(buf.createInputStream(), buf)
            resultsQueue.put(SuccessFetchResult(startPartition, pendingFetch.blockManagerId,
              inputStream))
          } else {
            buf.release()
          }
        }
        if (rdmaShuffleReaderStats != null) {
          rdmaShuffleReaderStats.updateRemoteFetchHistogram(pendingFetch.blockManagerId,
            (System.currentTimeMillis() - startRemoteFetchTime).toInt)
        }
        logTrace(s"Got ${blocksSize} " +
          s"remote block(s): of size ${totalBufferSize} " +
          s"from ${pendingFetch.blockManagerId} " +
          s"after ${Utils.getUsedTimeMs(startRemoteFetchTime)}")
      }

      override def onFailure(e: Throwable): Unit = {
        logError(s"Failed to getRdmaBlockLocation block(s) from: " +
          s"${pendingFetch.blockManagerId},\n Exception: $e")
        resultsQueue.put(FailureFetchResult(startPartition, pendingFetch.blockManagerId, e))

        rdmaRegisteredBuffer.release()
        // We skip curBytesInFlight since we expect one failure to fail the whole task
      }
    }

    val callbackId = channel.putCompletionListener(rdmaWriteListener)

    val msg = new RdmaWriteBlocks(callbackId, blocks.size, shuffleId,
      (rdmaRegisteredBuffer.getRegisteredAddress, rdmaRegisteredBuffer.getLkey),
      rdmaShuffleManager.getLocalRdmaShuffleManagerId, blocks)
    val prefetchMsgBuffers = msg.
      toRdmaByteBufferManagedBuffers(rdmaShuffleManager.getRdmaByteBufferManagedBuffer,
        rdmaShuffleConf.recvWrSize)
    val sendListener = new RdmaCompletionListener {
      override def onSuccess(buf: ByteBuffer): Unit = {
        logInfo(s"Sent prefetch message to ${pendingFetch.blockManagerId} " +
          s"for ${blocks.size} blocks, callbackId: $callbackId, took: "  +
          s"${Utils.getUsedTimeMs(startRemoteFetchTime)}")
        prefetchMsgBuffers.foreach(_.release())
      }

      override def onFailure(exception: Throwable): Unit = {
        logError(s"Failed to send prefetch Message ${exception}")
        exception.printStackTrace()
        prefetchMsgBuffers.foreach(_.release())
      }
    }

    channel.rdmaSendInQueue(sendListener, prefetchMsgBuffers.map(_.getAddress),
      prefetchMsgBuffers.map(_.getLkey), prefetchMsgBuffers.map(_.getLength.toInt))
  }

  private[this] def initialize(): Unit = {
    // Add a task completion callback (called in both success case and failure case) to cleanup.
    context.addTaskCompletionListener(_ => cleanup())

    splitPendingFetches(groupedBlocksByAddress)

    while (!pendingFetchesQueue.isEmpty &&
      curBytesInFlight.get() < rdmaShuffleConf.maxBytesInFlight) {
      sendPrefetchMsg()
    }

    for (partitionId <- startPartition until endPartition) {
      rdmaShuffleManager.shuffleBlockResolver.getLocalRdmaPartition(shuffleId, partitionId).foreach{
        case in: InputStream =>
          shuffleMetrics.incLocalBlocksFetched(1)
          shuffleMetrics.incLocalBytesRead(in.available())
          resultsQueue.put(SuccessFetchResult(partitionId, localBlockManagerId, in))

          numBlocksToFetch.incrementAndGet()
        case _ =>
      }
    }
  }

  override def hasNext: Boolean = numBlocksProcessed < numBlocksToFetch.get()

  override def next(): InputStream = {
    if (!hasNext) {
      return null
    }

    numBlocksProcessed += 1

    // logDebug(s"Number blocks processed: $numBlocksProcessed, " +
    //  s"number blocks to fetch: $numBlocksToFetch, flying messages: $flyingMessages")
    val startFetchWait = System.currentTimeMillis()
    currentResult = resultsQueue.take()
    val result = currentResult
    val stopFetchWait = System.currentTimeMillis()
    shuffleMetrics.incFetchWaitTime(stopFetchWait - startFetchWait)

    while (!pendingFetchesQueue.isEmpty &&
      curBytesInFlight.get() < rdmaShuffleConf.maxBytesInFlight) {
      sendPrefetchMsg()
    }

    result match {
      case SuccessFetchResult(_, blockManagerId, inputStream) =>
        if (inputStream != null && blockManagerId != localBlockManagerId) {
          shuffleMetrics.incRemoteBytesRead(inputStream.available)
          shuffleMetrics.incRemoteBlocksFetched(1)
        }
        inputStream
      case FailureMetadataFetchResult(e) => throw e
      case FailureFetchResult(partitionId, blockManagerId, e) =>
        throw new FetchFailedException(blockManagerId, shuffleId, 0, partitionId, e)
    }
  }

  override def toString(): String = {
    s"RdmaShuffleFetchIterator(shuffleId: $shuffleId, startPartition: $startPartition," +
      s"endPartition: $endPartition)"
  }
}

private class BufferReleasingInputStream(
    private val delegate: InputStream,
    private val buf: ManagedBuffer)
  extends InputStream {
  private[this] var closed = false

  override def read(): Int = delegate.read()

  override def close(): Unit = {
    if (!closed) {
      delegate.close()
      buf.release()
      closed = true
    }
  }

  override def available(): Int = delegate.available()

  override def mark(readlimit: Int): Unit = delegate.mark(readlimit)

  override def skip(n: Long): Long = delegate.skip(n)

  override def markSupported(): Boolean = delegate.markSupported()

  override def read(b: Array[Byte]): Int = delegate.read(b)

  override def read(b: Array[Byte], off: Int, len: Int): Int = delegate.read(b, off, len)

  override def reset(): Unit = delegate.reset()
}

private[rdma]
object RdmaShuffleFetcherIterator {

  private[rdma] sealed trait FetchResult { }

  private[rdma] case class SuccessFetchResult(
      partitionId: Int,
      blockManagerId: BlockManagerId,
      inputStream: InputStream) extends FetchResult

  private[rdma] case class FailureFetchResult(
      partitionId: Int,
      blockManagerId: BlockManagerId,
      e: Throwable) extends FetchResult

  private[rdma] case class FailureMetadataFetchResult(e: MetadataFetchFailedException)
      extends FetchResult
}
