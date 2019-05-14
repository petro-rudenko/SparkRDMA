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
package org.apache.spark.shuffle

import java.io.{Closeable, File, RandomAccessFile}
import java.nio.ByteBuffer
import java.nio.channels.FileChannel.MapMode
import java.util.concurrent.{ConcurrentHashMap, ConcurrentLinkedQueue}

import scala.collection.JavaConverters._

import org.openucx.jucx.{UcxCallback, UcxRequest}
import org.openucx.jucx.ucp.UcpMemory

import org.apache.spark.{SparkEnv, SparkException}
import org.apache.spark.internal.Logging
import org.apache.spark.shuffle.IndexShuffleBlockResolver.NOOP_REDUCE_ID
import org.apache.spark.storage.ShuffleIndexBlockId
import org.apache.spark.util.Utils


class UcxShuffleBlockResolver(ucxShuffleManager: UcxShuffleManager)
  extends IndexShuffleBlockResolver(ucxShuffleManager.conf) with Logging with Closeable {
  type MapId = Int

  private val fileMappings =
    new ConcurrentHashMap[ShuffleId, ConcurrentHashMap[MapId, UcpMemory]]
  private val offsetMappings =
    new ConcurrentHashMap[ShuffleId, ConcurrentHashMap[MapId, UcpMemory]]

  val resources =
    new ConcurrentHashMap[ShuffleId, ConcurrentLinkedQueue[Closeable]]()

  private def getIndexFile(shuffleId: Int, mapId: Int): File = {
    SparkEnv.get.blockManager
      .diskBlockManager.getFile(ShuffleIndexBlockId(shuffleId, mapId, NOOP_REDUCE_ID))
  }

  override def writeIndexFileAndCommit(shuffleId: ShuffleId, mapId: Int,
                                       lengths: Array[Long], dataTmp: File): Unit = {
    super.writeIndexFileAndCommit(shuffleId, mapId, lengths, dataTmp)
    val workerWrapper = ucxShuffleManager.ucxNode.getWorker
    val dataFile = getDataFile(shuffleId, mapId)
    val indexFile = getIndexFile(shuffleId, mapId)

    logInfo(s"Writing index file for mapId: $mapId," +
      s"workerId: ${workerWrapper.worker.getNativeId}")

    val dataBackFile = new RandomAccessFile(dataFile, "rw")
    val indexBackFile = new RandomAccessFile(indexFile, "rw")

    val dataFileChannel = dataBackFile.getChannel
    val indexFileChannel = indexBackFile.getChannel

    resources.computeIfAbsent(shuffleId,
      new java.util.function.Function[ShuffleId, ConcurrentLinkedQueue[Closeable]] {
        override def apply(t: ShuffleId): ConcurrentLinkedQueue[Closeable] =
          new ConcurrentLinkedQueue[Closeable]()
      }
    )

    fileMappings.computeIfAbsent(shuffleId,
      new java.util.function.Function[ShuffleId, ConcurrentHashMap[MapId, UcpMemory]] {
        override def apply(t: ShuffleId): ConcurrentHashMap[MapId, UcpMemory] =
          new ConcurrentHashMap[MapId, UcpMemory]()
      }
    )

    offsetMappings.computeIfAbsent(shuffleId,
      new java.util.function.Function[ShuffleId, ConcurrentHashMap[MapId, UcpMemory]] {
        override def apply(t: ShuffleId): ConcurrentHashMap[MapId, UcpMemory] =
          new ConcurrentHashMap[MapId, UcpMemory]()
      }
    )

    val fds = resources.get(shuffleId)
    Seq[Closeable](dataFileChannel, indexFileChannel,
      dataBackFile, indexBackFile).foreach(c => fds.add(c))

    val mappingStartTime = System.currentTimeMillis()
    val dataFileBuffer = dataFileChannel.map(MapMode.READ_WRITE, 0, dataBackFile.length())
      .load()
    val fileMemory = ucxShuffleManager.ucxNode.getContext.registerMemory(dataFileBuffer)
    fileMappings.get(shuffleId).put(mapId, fileMemory)

    val offsetBuffer = indexFileChannel.map(MapMode.READ_WRITE, 0, indexBackFile.length())
      .load()
    val offsetMemory = ucxShuffleManager.ucxNode.getContext.registerMemory(offsetBuffer)
    offsetMappings.get(shuffleId).put(mapId, offsetMemory)

    logInfo(s"Mapping buffers of size: ${dataFileBuffer.capacity()} b + " +
      s"${offsetBuffer.capacity()} b took ${Utils.getUsedTimeMs(mappingStartTime)}")

    val fileMemoryRkey = fileMemory.getRemoteKeyBuffer
    val offsetRkey = offsetMemory.getRemoteKeyBuffer

    logInfo(s"Offset rkey #${offsetRkey.hashCode()} for map $mapId")
    offsetRkey.clear()
    val metadataBuffer = ByteBuffer.allocateDirect(
      fileMemoryRkey.capacity() + offsetRkey.capacity() + 24)

    if (metadataBuffer.capacity() > ucxShuffleManager.ucxShuffleConf.metadataBlockSize) {
      throw new SparkException(s"Metadata block size ${metadataBuffer.capacity()} " +
        s"is greater then configured spark.shuffle.ucx.metadataBlockSize " +
        s"(${ucxShuffleManager.ucxShuffleConf.metadataBlockSize}).")
    }

    metadataBuffer.clear()

    metadataBuffer.putLong(offsetMemory.getAddress)
    metadataBuffer.putLong(fileMemory.getAddress)

    metadataBuffer.putInt(offsetRkey.capacity())
    metadataBuffer.putInt(fileMemoryRkey.capacity())

    metadataBuffer.put(offsetRkey)
    metadataBuffer.put(fileMemoryRkey)

    metadataBuffer.clear()


    val driverMetadata = workerWrapper.driverMetadaBuffer(shuffleId)
    val driverOffset = driverMetadata.address +
      mapId * ucxShuffleManager.ucxShuffleConf.metadataBlockSize

    val driverEndpoint = workerWrapper.driverEndpoint

    logInfo(s"Writing to driver address: $driverOffset," +
      s"buffer of size: ${metadataBuffer.capacity()}, worker: ${workerWrapper.worker.getNativeId}")
    val request = driverEndpoint.putNonBlocking(metadataBuffer, driverOffset,
      driverMetadata.ucpRkey, new UcxCallback() {
        private val startTime = System.currentTimeMillis()

        override def onSuccess(request: UcxRequest): Unit = {
          logInfo(s"RDMA write mapID: $mapId to driver address($driverOffset) buffer of size: " +
            s"${metadataBuffer.limit()} took ${Utils.getUsedTimeMs(startTime)}")
        }
      })

    try {
      workerWrapper.preConnect()
    } catch {
      case exception: Exception => logWarning(s"Failed to establish connection:" +
        s"${exception.getLocalizedMessage}")
    }

    workerWrapper.progressRequest(request)
    ucxShuffleManager.ucxNode.putWorker(workerWrapper)
  }

  def removeShuffle(shuffleId: Int): Unit = {
    logInfo(s"Removing shuffle $shuffleId")
    fileMappings.remove(shuffleId).asScala.values.foreach(_.deregister())
    offsetMappings.remove(shuffleId).asScala.values.foreach(_.deregister())
    resources.asScala.remove(shuffleId).foreach {
      c: ConcurrentLinkedQueue[Closeable] => c.asScala.foreach(_.close())
    }
    logInfo(s"Removed shuffle $shuffleId")
  }

  override def close(): Unit = {
    logInfo(s"Stoping ShuffleBlockResolver ${fileMappings.asScala.keys.mkString(",")}")
    fileMappings.asScala.keys.foreach(removeShuffle)
  }
}
