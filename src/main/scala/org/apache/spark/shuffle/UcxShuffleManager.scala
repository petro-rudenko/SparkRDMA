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
import java.util.concurrent.atomic.AtomicReferenceArray

import scala.collection.JavaConverters._

import org.openucx.jucx.{UcxCallback, UcxRequest}
import org.openucx.jucx.ucp.{UcpMemory, UcpRemoteKey}

import org.apache.spark.{ShuffleDependency, SparkConf, TaskContext}
import org.apache.spark.shuffle.sort.{SerializedShuffleHandle, SortShuffleManager}
import org.apache.spark.shuffle.ucx.{UcxDriverNode, UcxExecutorNode, UcxNode, UcxShuffleClient}
import org.apache.spark.shuffle.ucx.rpc.UcxRemoteMemory
import org.apache.spark.unsafe.Platform
import org.apache.spark.util.{ShutdownHookManager, Utils}

case class DriverMetadaBuffer(address: Long, ucpRkey: UcpRemoteKey, var length: Int,
                              var data: ByteBuffer)

class UcxShuffleManager(val conf: SparkConf, isDriver: Boolean)
  extends SortShuffleManager(conf) {
  type ShuffleId = Int
  type MapId = Int

  ShutdownHookManager.addShutdownHook(Int.MaxValue)(stop)

  private val shuffleIdToMetadataBuffer =
    new ConcurrentHashMap[ShuffleId, UcpMemory]().asScala

  private var ucxNode: Option[UcxNode] = None

  val ucxShuffleConf = new UcxShuffleConf(conf)

  final val driverMetadaBuffer: scala.collection.concurrent.Map[ShuffleId, DriverMetadaBuffer] =
    new ConcurrentHashMap[ShuffleId, DriverMetadaBuffer]().asScala

  final val offsetRkeyCache = new ConcurrentHashMap[ShuffleId,
    AtomicReferenceArray[UcpRemoteKey]]().asScala

  final val dataRkeyCache = new ConcurrentHashMap[ShuffleId,
    AtomicReferenceArray[UcpRemoteKey]]().asScala

  final val shuffleIdToShuffleClient = new ConcurrentHashMap[ShuffleId, UcxShuffleClient]().asScala

  private def startUcxNodeIfMissing(): Unit = {
    assume(!isDriver)
    synchronized{
      if (ucxNode.isEmpty) {
        val node = new UcxExecutorNode(ucxShuffleConf)
        ucxNode = Some(node)
      }
    }
  }

  // Called on the driver only!
  override def registerShuffle[K, V, C](shuffleId: Int,
                                        numMaps: Int,
                                        dependency: ShuffleDependency[K, V, C]): ShuffleHandle = {
    assume(isDriver)
    if (ucxNode.isEmpty) {
      val node = new UcxDriverNode(ucxShuffleConf)
      node.startDriverListener()
      ucxNode = Some(node)
    }
    val metadataBuffer = Platform.allocateDirectBuffer(
      numMaps * ucxShuffleConf.metadataBlockSize.toInt)

    val metadataMemory = ucxNode.get.getContext.registerMemory(metadataBuffer)
    shuffleIdToMetadataBuffer.put(shuffleId, metadataMemory)

    val driverMemory = new UcxRemoteMemory(metadataMemory.getAddress,
      metadataMemory.getRemoteKeyBuffer)
    logInfo(s"Rkey #${driverMemory.getrKeyBuffer().hashCode()}")
    metadataMemory.getRemoteKeyBuffer.clear()

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
    driverMetadaBuffer.putIfAbsent(handle.shuffleId, {
        val (address, length, rkey): (Long, Int, ByteBuffer) =
          if (handle.isInstanceOf[UcxBaseShuffleHandle[_, _, _]]) {
            val ucxShuffleHandle = handle.asInstanceOf[UcxBaseShuffleHandle[K, V, _]]
            (ucxShuffleHandle.metadataArrayOnDriver.getAddress,
              ucxShuffleHandle.numMaps * ucxShuffleConf.metadataBlockSize.toInt,
              ucxShuffleHandle.metadataArrayOnDriver.getrKeyBuffer())
          } else {
            val ucxShuffleHandle = handle.asInstanceOf[UcxSerializedShuffleHandle[K, V]]
            (ucxShuffleHandle.metadataArrayOnDriver.getAddress,
              ucxShuffleHandle.numMaps * ucxShuffleConf.metadataBlockSize.toInt,
              ucxShuffleHandle.metadataArrayOnDriver.getrKeyBuffer())
          }
        rkey.clear()
        logInfo(s"Driver address: $address rkey #${rkey.hashCode()}, size: $length")
        rkey.clear()
        val unpackedRkey = getUcxNode.asInstanceOf[UcxExecutorNode]
          .getDriverEndpoint.unpackRemoteKey(rkey)
        DriverMetadaBuffer(address, unpackedRkey, length, null)
      })
    super.getWriter(handle, mapId, context)
  }

  override def getReader[K, C](handle: ShuffleHandle, startPartition: Int,
                               endPartition: Int, context: TaskContext): ShuffleReader[K, C] = {
    startUcxNodeIfMissing()
    val numBlocks = handle.asInstanceOf[BaseShuffleHandle[_, _, _]].numMaps
    offsetRkeyCache.putIfAbsent(handle.shuffleId, new AtomicReferenceArray[UcpRemoteKey](numBlocks))
    dataRkeyCache.putIfAbsent(handle.shuffleId, new AtomicReferenceArray[UcpRemoteKey](numBlocks))

    driverMetadaBuffer.putIfAbsent(handle.shuffleId, {
      val (address, length, rkey): (Long, Int, ByteBuffer) =
        if (handle.isInstanceOf[UcxBaseShuffleHandle[_, _, _]]) {
          val ucxShuffleHandle = handle.asInstanceOf[UcxBaseShuffleHandle[K, _, C]]
          (ucxShuffleHandle.metadataArrayOnDriver.getAddress,
            ucxShuffleHandle.numMaps * ucxShuffleConf.metadataBlockSize.toInt,
            ucxShuffleHandle.metadataArrayOnDriver.getrKeyBuffer())
        } else {
          val ucxShuffleHandle = handle.asInstanceOf[UcxSerializedShuffleHandle[K, _]]
          (ucxShuffleHandle.metadataArrayOnDriver.getAddress,
            ucxShuffleHandle.numMaps * ucxShuffleConf.metadataBlockSize.toInt,
            ucxShuffleHandle.metadataArrayOnDriver.getrKeyBuffer())
        }
      rkey.clear()
      val unpackedRkey = getUcxNode.asInstanceOf[UcxExecutorNode]
        .getDriverEndpoint.unpackRemoteKey(rkey)
      DriverMetadaBuffer(address, unpackedRkey, length, null)
    })
    val driverMetadata = driverMetadaBuffer(handle.shuffleId)
    if (driverMetadata.data == null) {
      synchronized {
        if (driverMetadata.data == null) {
          driverMetadata.data = Platform.allocateDirectBuffer(driverMetadata.length)

          val currentClass = this
          val request = ucxNode.get.asInstanceOf[UcxExecutorNode].getDriverEndpoint.getNonBlocking(
            driverMetadata.address, driverMetadata.ucpRkey, driverMetadata.data,
            new UcxCallback() {
              private val startTime = System.currentTimeMillis()

              override def onSuccess(request: UcxRequest): Unit = {
                currentClass.synchronized {
                  currentClass.notify()
                }
                logInfo(s"Get Driver metadata from address ${driverMetadata.address}" +
                  s" buffer of size: ${driverMetadata.length}" +
                  s" took ${Utils.getUsedTimeMs(startTime)}")
              }
            }
          )
          while (!request.isCompleted) {
            currentClass.wait()
          }
        }
      }
    }
    val shuffleClient = shuffleIdToShuffleClient.getOrElseUpdate(handle.shuffleId,
      new UcxShuffleClient(handle.shuffleId))
    new UcxShuffleReader(handle.asInstanceOf[BaseShuffleHandle[K, _, C]], startPartition,
      endPartition, shuffleClient, context)
  }

  override val shuffleBlockResolver: UcxShuffleBlockResolver = new UcxShuffleBlockResolver(this)

  override def unregisterShuffle(shuffleId: Int): Boolean = {
    if (!isDriver) {
      logInfo(s"Unregistering shuffle $shuffleId")
      shuffleBlockResolver.removeShuffle(shuffleId)
      driverMetadaBuffer.remove(shuffleId).foreach(d => d.ucpRkey.close())
      offsetRkeyCache.remove(shuffleId).foreach(seq => {
        (0 until seq.length()).foreach(i => {
          val dataRkey = seq.get(i)
          if (dataRkey != null) {
            dataRkey.close()
          }
        })
      })
      dataRkeyCache.remove(shuffleId).foreach(seq => {
        (0 until seq.length()).foreach(i => {
          val offsetRkey = seq.get(i)
          if (offsetRkey != null) {
            offsetRkey.close()
          }
        })
      })
    } else {
      shuffleIdToMetadataBuffer.remove(shuffleId).foreach(_.deregister())
    }
    super.unregisterShuffle(shuffleId)
  }

  override def stop(): Unit = synchronized {
    logInfo("Stopping shuffle manager")
    if (!isDriver) {
      driverMetadaBuffer.keySet.foreach(unregisterShuffle)
      driverMetadaBuffer.clear()
    }
    shuffleIdToMetadataBuffer.values.foreach(_.deregister())
    shuffleIdToMetadataBuffer.clear()
    shuffleIdToShuffleClient.values.foreach(_.close())
    shuffleIdToShuffleClient.clear()
    if (ucxNode.isDefined) {
      ucxNode.get.close()
      ucxNode = None
    }
  }

  def getUcxNode: UcxNode = {
    ucxNode.get
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
