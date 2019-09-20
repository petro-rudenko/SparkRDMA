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

import java.nio.ByteBuffer
import java.util.concurrent.ConcurrentHashMap

import scala.collection.JavaConverters._

import org.openucx.jucx.ucp.{UcpMemory, UcpRemoteKey}

import org.apache.spark.{ShuffleDependency, SparkConf, TaskContext}
import org.apache.spark.shuffle.sort.{SerializedShuffleHandle, SortShuffleManager}
import org.apache.spark.shuffle.ucx.{UcxNode, UcxShuffleClient}
import org.apache.spark.shuffle.ucx.rpc.UcxRemoteMemory
import org.apache.spark.unsafe.Platform
import org.apache.spark.util.ShutdownHookManager

case class DriverMetadaBuffer(address: Long, ucpRkey: UcpRemoteKey, var length: Int,
                              var data: ByteBuffer)

class UcxShuffleManager(val conf: SparkConf, isDriver: Boolean)
  extends SortShuffleManager(conf) {
  type ShuffleId = Int
  type MapId = Int

  val ucxShuffleConf = new UcxShuffleConf(conf)
  var ucxNode: UcxNode = _

  ShutdownHookManager.addShutdownHook(Int.MaxValue - 1)(stop)

  private val shuffleIdToMetadataBuffer =
    new ConcurrentHashMap[ShuffleId, UcpMemory]().asScala

  val shuffleIdToHandle = new ConcurrentHashMap[ShuffleId, ShuffleHandle]().asScala

  def startUcxNodeIfMissing(): Unit = {
    synchronized {
      if (ucxNode == null) {
        ucxNode = new UcxNode(ucxShuffleConf, isDriver)
      }
    }
  }

  if (isDriver) {
    startUcxNodeIfMissing()
  }

  // Called on the driver only!
  override def registerShuffle[K, V, C](shuffleId: Int,
                                        numMaps: Int,
                                        dependency: ShuffleDependency[K, V, C]): ShuffleHandle = {
    assume(isDriver)
    startUcxNodeIfMissing()
    val metadataBuffer = Platform.allocateDirectBuffer(
      numMaps * ucxShuffleConf.metadataBlockSize.toInt)

    val metadataMemory = ucxNode.getContext.registerMemory(metadataBuffer)
    shuffleIdToMetadataBuffer.put(shuffleId, metadataMemory)

    val driverMemory = new UcxRemoteMemory(metadataMemory.getAddress,
      metadataMemory.getRemoteKeyBuffer)

    logInfo(s"Metadata memory address: ${metadataMemory.getAddress}," +
      s"size: ${metadataBuffer.capacity()}")

    // BypassMergeSortShuffleWriter is not supported since it is package private
    if (SortShuffleManager.canUseSerializedShuffle(dependency)) {
      // Otherwise, try to buffer map outputs in a serialized form, since this is more efficient:
      new UcxSerializedShuffleHandle[K, V](driverMemory, shuffleId, numMaps,
        dependency.asInstanceOf[ShuffleDependency[K, V, V]])
    } else {
      // Otherwise, buffer map outputs in a deserialized form:
      new UcxBaseShuffleHandle(driverMemory, shuffleId, numMaps, dependency)
    }
  }

  override def getWriter[K, V](handle: ShuffleHandle, mapId: Int,
                               context: TaskContext): ShuffleWriter[K, V] = {
    startUcxNodeIfMissing()
    shuffleIdToHandle.putIfAbsent(handle.shuffleId, handle)
    super.getWriter(handle, mapId, context)
  }

  override def getReader[K, C](handle: ShuffleHandle, startPartition: Int,
                               endPartition: Int, context: TaskContext): ShuffleReader[K, C] = {
    startUcxNodeIfMissing()
    new UcxShuffleReader(handle.asInstanceOf[BaseShuffleHandle[K, _, C]], startPartition,
      endPartition, context)
  }

  override val shuffleBlockResolver: UcxShuffleBlockResolver = new UcxShuffleBlockResolver(this)

  override def unregisterShuffle(shuffleId: Int): Boolean = {
    if (!isDriver) {
      logInfo(s"Unregistering shuffle $shuffleId")
      shuffleBlockResolver.removeShuffle(shuffleId)
    } else {
      shuffleIdToMetadataBuffer.remove(shuffleId).foreach(_.deregister())
    }
    super.unregisterShuffle(shuffleId)
  }

  override def stop(): Unit = synchronized {
    logInfo("Stopping shuffle manager")
    if (!isDriver) {
      shuffleBlockResolver.close()
    }
    shuffleIdToMetadataBuffer.values.foreach(_.deregister())
    shuffleIdToMetadataBuffer.clear()
    if (ucxNode != null) {
      ucxNode.close()
      ucxNode = null
    }
    super.stop()
  }

}

class UcxBaseShuffleHandle[K, V, C](val metadataArrayOnDriver: UcxRemoteMemory,
                                    shuffleId: Int,
                                    numMaps: Int,
                                    dependency: ShuffleDependency[K, V, C])
  extends BaseShuffleHandle[K, V, C](shuffleId, numMaps, dependency)

class UcxSerializedShuffleHandle[K, V](val metadataArrayOnDriver: UcxRemoteMemory,
                                       shuffleId: Int,
                                       numMaps: Int,
                                       dependency: ShuffleDependency[K, V, V])
  extends SerializedShuffleHandle[K, V](shuffleId, numMaps, dependency)
