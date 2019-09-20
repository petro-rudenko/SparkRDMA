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
import java.util.concurrent.{ConcurrentHashMap, ConcurrentLinkedQueue}

import scala.collection.JavaConverters._

import org.openucx.jucx.UcxUtils
import org.openucx.jucx.ucp.UcpMemory

import org.apache.spark.{SparkEnv, SparkException}
import org.apache.spark.internal.Logging
import org.apache.spark.shuffle.IndexShuffleBlockResolver.NOOP_REDUCE_ID
import org.apache.spark.shuffle.ucx.UnsafeUtils
import org.apache.spark.storage.ShuffleIndexBlockId
import org.apache.spark.util.Utils


class UcxShuffleBlockResolver(ucxShuffleManager: UcxShuffleManager)
  extends IndexShuffleBlockResolver(ucxShuffleManager.conf) with Logging with Closeable {
  type MapId = Int

  private lazy val memPool = ucxShuffleManager.ucxNode.getMemoryPool

  // Keep track of opened and mmaped files to close it when shuffle not needed
  private val fileMappings =
    new ConcurrentHashMap[ShuffleId, ConcurrentHashMap[MapId, UcpMemory]].asScala

  private val offsetMappings =
    new ConcurrentHashMap[ShuffleId, ConcurrentHashMap[MapId, UcpMemory]].asScala

  private val resources =
    new ConcurrentHashMap[ShuffleId, ConcurrentLinkedQueue[Closeable]]().asScala

  private def getIndexFile(shuffleId: Int, mapId: Int): File = {
    SparkEnv.get.blockManager
      .diskBlockManager.getFile(ShuffleIndexBlockId(shuffleId, mapId, NOOP_REDUCE_ID))
  }

  override def writeIndexFileAndCommit(shuffleId: ShuffleId, mapId: Int,
                                       lengths: Array[Long], dataTmp: File): Unit = {
    super.writeIndexFileAndCommit(shuffleId, mapId, lengths, dataTmp)

    val dataFile = getDataFile(shuffleId, mapId)
    val dataBackFile = new RandomAccessFile(dataFile, "rw")

    if (dataBackFile.length() == 0) {
      return
    }
    resources.putIfAbsent(shuffleId, new ConcurrentLinkedQueue[Closeable]())
    fileMappings.putIfAbsent(shuffleId, new ConcurrentHashMap[MapId, UcpMemory]())
    offsetMappings.putIfAbsent(shuffleId, new ConcurrentHashMap[MapId, UcpMemory]())

    val s = System.currentTimeMillis()
    logInfo(s"Getting worker")
    val workerWrapper = ucxShuffleManager.ucxNode.getWorker
    logInfo(s"Took worker ${workerWrapper.id}")
    workerWrapper.addDriverMetadata(ucxShuffleManager.shuffleIdToHandle(shuffleId))

    val indexFile = getIndexFile(shuffleId, mapId)
    val indexBackFile = new RandomAccessFile(indexFile, "rw")
    val indexFileChannel = indexBackFile.getChannel

    val dataFileChannel = dataBackFile.getChannel
    val fds = resources(shuffleId)
    Seq[Closeable](dataFileChannel, indexFileChannel,
      dataBackFile, indexBackFile).foreach(c => fds.add(c))

    val mappingStartTime = System.currentTimeMillis()
    val dataFileBuffer = UnsafeUtils.mmap(dataFileChannel, 0, dataBackFile.length())
    val fileMemory = ucxShuffleManager.ucxNode.getContext.registerMemory(dataFileBuffer)
    fileMappings(shuffleId).put(mapId, fileMemory)

    logInfo(s"MapID: $mapId Mapping data file took ${Utils.getUsedTimeMs(mappingStartTime)}")

    val offsetBuffer = UnsafeUtils.mmap(indexFileChannel, 0, indexBackFile.length())
    val offsetMemory = ucxShuffleManager.ucxNode.getContext.registerMemory(offsetBuffer)
    offsetMappings(shuffleId).put(mapId, offsetMemory)

    logInfo(s"MapID: $mapId Mapping buffers of size: ${dataFileBuffer.capacity()} b + " +
      s"${offsetBuffer.capacity()} b took ${Utils.getUsedTimeMs(mappingStartTime)}")

    val fileMemoryRkey = fileMemory.getRemoteKeyBuffer
    val offsetRkey = offsetMemory.getRemoteKeyBuffer

    logInfo(s"Offset rkey #${offsetRkey.hashCode()} for map $mapId")
    offsetRkey.clear()
    val metadataRegisteredMemory = memPool.get(
      fileMemoryRkey.capacity() + offsetRkey.capacity() + 24)
    val metadataBuffer = metadataRegisteredMemory.getBuffer.slice()

    if (metadataBuffer.remaining() > ucxShuffleManager.ucxShuffleConf.metadataBlockSize) {
      throw new SparkException(s"Metadata block size ${metadataBuffer.remaining()} " +
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
    val request = driverEndpoint.putNonBlocking(UcxUtils.getAddress(metadataBuffer),
      metadataBuffer.remaining(), driverOffset, driverMetadata.ucpRkey, null)

    try {
      workerWrapper.preConnect()
      workerWrapper.progressRequest(request)
      memPool.put(metadataRegisteredMemory)
    } catch {
      case exception: Exception => logWarning(s"Failed to establish connection:" +
        s"${exception.getLocalizedMessage}")
        workerWrapper.clearConnections()
    }

    logInfo(s"Return worker ${workerWrapper.id}")
    ucxShuffleManager.ucxNode.putWorker(workerWrapper)
    logInfo(s"MapID: $mapId Total overhead: ${Utils.getUsedTimeMs(s)}")
  }

  def removeShuffle(shuffleId: Int): Unit = {
    logInfo(s"Removing shuffle $shuffleId")
    fileMappings.remove(shuffleId).foreach(_.asScala.values.foreach({
      mem: UcpMemory =>
        val address = mem.getAddress
        val length = mem.getData.capacity()
        mem.deregister()
        UnsafeUtils.munmap(address, length)
    }))
    offsetMappings.remove(shuffleId).foreach(_.values().asScala.foreach(_.deregister()))
    resources.remove(shuffleId).foreach {
      c: ConcurrentLinkedQueue[Closeable] => c.asScala.foreach(_.close())
    }
    logInfo(s"Removed shuffle $shuffleId")
  }

  override def close(): Unit = {
    logInfo(s"Stoping ShuffleBlockResolver ${fileMappings.keys.mkString(",")}")
    fileMappings.keys.foreach(removeShuffle)
  }
}
