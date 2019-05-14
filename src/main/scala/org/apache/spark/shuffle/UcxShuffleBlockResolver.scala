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
import java.nio.{ByteBuffer, ByteOrder}
import java.nio.channels.FileChannel.MapMode
import java.util.concurrent.{ConcurrentHashMap, ConcurrentLinkedQueue}

import scala.collection.JavaConverters._

import org.openucx.jucx.{UcxCallback, UcxRequest}
import org.openucx.jucx.ucp.UcpMemory

import org.apache.spark.{SparkEnv, SparkException}
import org.apache.spark.internal.Logging
import org.apache.spark.shuffle.IndexShuffleBlockResolver.NOOP_REDUCE_ID
import org.apache.spark.shuffle.ucx.UcxExecutorNode
import org.apache.spark.storage.ShuffleIndexBlockId
import org.apache.spark.util.Utils


class UcxShuffleBlockResolver(ucxShuffleManager: UcxShuffleManager)
  extends IndexShuffleBlockResolver(ucxShuffleManager.conf) with Logging {

  private val fileMappings = new ConcurrentHashMap[ShuffleId, UcpMemory].asScala
  private val offsetMappings = new ConcurrentHashMap[ShuffleId, UcpMemory].asScala

  val resources =
    new ConcurrentHashMap[ShuffleId, ConcurrentLinkedQueue[Closeable]]()

  private def getIndexFile(shuffleId: Int, mapId: Int): File = {
    SparkEnv.get.blockManager
      .diskBlockManager.getFile(ShuffleIndexBlockId(shuffleId, mapId, NOOP_REDUCE_ID))
  }

  override def writeIndexFileAndCommit(shuffleId: ShuffleId, mapId: Int,
                                       lengths: Array[Long], dataTmp: File): Unit = {
    super.writeIndexFileAndCommit(shuffleId, mapId, lengths, dataTmp)
    val dataFile = getDataFile(shuffleId, mapId)
    val indexFile = getIndexFile(shuffleId, mapId)

    val dataBackFile = new RandomAccessFile(dataFile, "rw");
    val indexBackFile = new RandomAccessFile(indexFile, "rw");

    val dataFileChannel = dataBackFile.getChannel
    val indexFileChannel = indexBackFile.getChannel

    resources.computeIfAbsent(shuffleId,
      new java.util.function.Function[ShuffleId, ConcurrentLinkedQueue[Closeable]] {
        override def apply(t: ShuffleId): ConcurrentLinkedQueue[Closeable] =
          new ConcurrentLinkedQueue[Closeable]()
      }
    )

    val fds = resources.get(shuffleId)
    Seq[Closeable](dataFileChannel, indexFileChannel,
      dataBackFile, indexBackFile).foreach(c => fds.add(c))

    val mappingStartTime = System.currentTimeMillis()
    val dataFileBuffer = dataFileChannel.map(MapMode.READ_WRITE, 0, dataBackFile.length())
      .load()
    val fileMemory = ucxShuffleManager.getUcxNode.getContext.registerMemory(dataFileBuffer)
    fileMappings.put(shuffleId, fileMemory)

    val offsetBuffer = indexFileChannel.map(MapMode.READ_WRITE, 0, indexBackFile.length())
      .load()
    val offsetMemory = ucxShuffleManager.getUcxNode.getContext.registerMemory(offsetBuffer)

    logInfo(s"Mapping buffers of size: ${dataFileBuffer.capacity()} b + " +
      s"${offsetBuffer.capacity()} b took ${Utils.getUsedTimeMs(mappingStartTime)}")

    val fileMemoryRkey = fileMemory.getRemoteKeyBuffer
    val offsetRkey = offsetMemory.getRemoteKeyBuffer

    logInfo(s"Offset rkey #${offsetRkey.hashCode()} for map ${mapId}")
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

    val driverMetadata = ucxShuffleManager.driverMetadaBuffer(shuffleId)
    val driverOffset = driverMetadata.address +
      mapId * ucxShuffleManager.ucxShuffleConf.metadataBlockSize

    val driverEndpoint = ucxShuffleManager.getUcxNode.asInstanceOf[UcxExecutorNode]
        .getDriverEndpoint

    driverEndpoint.putNonBlocking(metadataBuffer, driverOffset,
      driverMetadata.ucpRkey, new UcxCallback() {
        val startTime = System.currentTimeMillis()

        override def onSuccess(request: UcxRequest): Unit = {
          logInfo(s"RDMA write mapID: $mapId to driver address($driverOffset) buffer of size: " +
            s"${metadataBuffer.limit()} took ${Utils.getUsedTimeMs(startTime)}")
        }
      })
  }

  def removeShuffle(shuffleId: Int): Unit = {
    fileMappings.get(shuffleId).foreach(_.deregister())
    offsetMappings.get(shuffleId).foreach(_.deregister())
    resources.asScala.get(shuffleId).foreach{
      case c: ConcurrentLinkedQueue[Closeable] => c.peek().close()
    }
  }
}
