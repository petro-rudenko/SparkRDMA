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

import java.io.Closeable
import java.net.InetSocketAddress
import java.nio.ByteBuffer

import scala.collection.JavaConverters._
import scala.collection.mutable

import org.openucx.jucx.UcxRequest
import org.openucx.jucx.ucp.{UcpEndpoint, UcpEndpointParams, UcpRemoteKey, UcpWorker}

import org.apache.spark.SparkEnv
import org.apache.spark.internal.Logging
import org.apache.spark.storage.{BlockManager, BlockManagerId}
import org.apache.spark.unsafe.Platform
import org.apache.spark.util.Utils

class UcxWorkerWrapper(val worker: UcpWorker, val conf: UcxShuffleConf) extends Closeable
  with Logging {
  type ShuffleId = Int
  type MapId = Int

  private final val socketAddress = new InetSocketAddress(conf.driverHost, conf.driverPort)
  private final val endpointParams = new UcpEndpointParams().setSocketAddress(socketAddress)
    .setPeerErrorHadnlingMode()
  val driverEndpoint: UcpEndpoint = worker.newEndpoint(endpointParams)

  final val driverMetadaBuffer = mutable.Map.empty[ShuffleId, DriverMetadaBuffer]

  final val offsetRkeyCache = mutable.Map.empty[ShuffleId, Array[UcpRemoteKey]]

  final val dataRkeyCache = mutable.Map.empty[ShuffleId, Array[UcpRemoteKey]]

  val blockManager: BlockManager = SparkEnv.get.blockManager

  private final val connections = mutable.Map.empty[BlockManagerId, UcpEndpoint]

  def preConnect(): Unit = {
    blockManager.master.getPeers(blockManager.blockManagerId).foreach(getConnection)
  }

  override def close(): Unit = {
    driverMetadaBuffer.values.foreach(_.ucpRkey.close())
    driverMetadaBuffer.clear()
    offsetRkeyCache.mapValues(_.foreach(_.close()))
    offsetRkeyCache.clear()
    dataRkeyCache.mapValues(_.foreach(_.close()))
    dataRkeyCache.clear()
    driverEndpoint.close()
    worker.close()
  }

  def addDriverMetadata(handle: ShuffleHandle): Unit = {
    driverMetadaBuffer.getOrElseUpdate(handle.shuffleId, {
      val (address, length, rkey): (Long, Int, ByteBuffer) =
        handle match {
          case ucxShuffleHandle: UcxBaseShuffleHandle[_, _, _] =>
            (ucxShuffleHandle.metadataArrayOnDriver.getAddress,
              ucxShuffleHandle.numMaps * conf.metadataBlockSize.toInt,
              ucxShuffleHandle.metadataArrayOnDriver.getrKeyBuffer())
          case _ =>
            val ucxShuffleHandle = handle.asInstanceOf[UcxSerializedShuffleHandle[_, _]]
            (ucxShuffleHandle.metadataArrayOnDriver.getAddress,
              ucxShuffleHandle.numMaps * conf.metadataBlockSize.toInt,
              ucxShuffleHandle.metadataArrayOnDriver.getrKeyBuffer())
        }
      rkey.clear()
      val unpackedRkey = driverEndpoint.unpackRemoteKey(rkey)
      DriverMetadaBuffer(address, unpackedRkey, length, null)
    })
  }

  def fetchDriverMetadataBuffer(handle: ShuffleHandle): DriverMetadaBuffer = {
    val numBlocks = handle.asInstanceOf[BaseShuffleHandle[_, _, _]].numMaps
    offsetRkeyCache.getOrElseUpdate(handle.shuffleId, Array.ofDim[UcpRemoteKey](numBlocks))
    dataRkeyCache.getOrElseUpdate(handle.shuffleId, Array.ofDim[UcpRemoteKey](numBlocks))
    val driverMetadata = driverMetadaBuffer(handle.shuffleId)

    if (driverMetadata.data == null) {
      driverMetadata.data = Platform.allocateDirectBuffer(driverMetadata.length)
      val request = driverEndpoint.getNonBlocking(
        driverMetadata.address, driverMetadata.ucpRkey, driverMetadata.data, null)
      progressRequest(request)
    }
    driverMetadata
  }

  def progressRequest(request: UcxRequest): Unit = {
    val startTime = System.currentTimeMillis()
    while (!request.isCompleted) {
      progress()
    }
    logTrace(s"Request completed in ${Utils.getUsedTimeMs(startTime)}")
  }

  def progressRequests(requests: java.util.Collection[UcxRequest], numRequests: Int): Unit = {
    val startTime = System.currentTimeMillis()
    while (!requests.asScala.forall(_.isCompleted) || requests.size() != numRequests) {
      progress()
    }
    logTrace(s"$numRequests completed in ${Utils.getUsedTimeMs(startTime)}")
  }


  /**
   * The only place for worker progress
   */
  private def progress(): Int = {
    try {
      worker.progress()
    } catch {
      case ex: Exception =>
        logError(s"Exception during worker(${worker.getNativeId}) progress: ${ex.getMessage}")
        ex.printStackTrace()
        0
    }
  }

  def getConnection(blockManagerId: BlockManagerId): UcpEndpoint = {
    connections.getOrElseUpdate(blockManagerId, {
      logInfo(s"Creating connection to $blockManagerId")
      val endpointParams = new UcpEndpointParams()
        .setPeerErrorHadnlingMode()
        .setSocketAddress(new InetSocketAddress(blockManagerId.host, blockManagerId.port + 7))
      val endpoint = worker.newEndpoint(endpointParams)
      progress()
      endpoint
    })
  }
}
