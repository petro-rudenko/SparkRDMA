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

import java.io.{DataInputStream, DataOutputStream, EOFException}
import java.nio.ByteBuffer

import org.slf4j.LoggerFactory
import scala.collection.mutable.ArrayBuffer

import org.apache.spark.internal.Logging
import org.apache.spark.shuffle.rdma.RdmaRpcMsgType.RdmaRpcMsgType
import org.apache.spark.storage.ShuffleBlockId

object RdmaRpcMsgType extends Enumeration {
  type RdmaRpcMsgType = Value
  val RdmaShuffleManagerHello, AnnounceRdmaShuffleManagers, RdmaPrefetchRPCMessage = Value
}

trait RdmaRpcMsg extends Logging{
  protected def msgType: RdmaRpcMsgType
  protected def getLengthInSegments(segmentSize: Int): Array[Int]
  protected def read(dataIn: DataInputStream): Unit
  protected def writeSegments(outs: Iterator[(DataOutputStream, Int)]): Unit

  private final val overhead: Int = 4 + 4 // 4 + 4 for msg length and type

  def toRdmaByteBufferManagedBuffers(allocator: Int => RdmaByteBufferManagedBuffer,
      maxSegmentSize: Int): Array[RdmaByteBufferManagedBuffer] = {
    val arrSegmentLengths = getLengthInSegments(maxSegmentSize - overhead)
    val bufs = Array.fill(arrSegmentLengths.length) { allocator(maxSegmentSize) }
    val outs = for ((buf, bufferIndex) <- bufs.zipWithIndex) yield {
      val out = new DataOutputStream(buf.createOutputStream())
      out.writeInt(overhead + arrSegmentLengths(bufferIndex))
      out.writeInt(msgType.id)
      (out, arrSegmentLengths(bufferIndex))
    }

    writeSegments(outs.iterator)
    outs.foreach(_._1.close())

    bufs
  }
}

object RdmaRpcMsg extends Logging {
  private val logger = LoggerFactory.getLogger(classOf[RdmaRpcMsg])

  def apply(buf: ByteBuffer): RdmaRpcMsg = {
    val in = new DataInputStream(new ByteBufferBackedInputStream(buf))
    val msgLength = in.readInt()
    buf.limit(msgLength)

    RdmaRpcMsgType(in.readInt()) match {
      case RdmaRpcMsgType.RdmaShuffleManagerHello =>
        RdmaShuffleManagerHelloRpcMsg(in)
      case RdmaRpcMsgType.AnnounceRdmaShuffleManagers =>
        RdmaAnnounceRdmaShuffleManagersRpcMsg(in)
      case RdmaRpcMsgType.RdmaPrefetchRPCMessage =>
        RdmaWriteBlocks(in)
      case _ =>
        logger.warn("Received an unidentified RPC")
        null
    }
  }
}

class RdmaShuffleManagerHelloRpcMsg(var rdmaShuffleManagerId: RdmaShuffleManagerId,
    var channelPort: Int) extends RdmaRpcMsg {
  private def this() = this(null, 0)  // For deserialization only

  override protected def msgType: RdmaRpcMsgType = RdmaRpcMsgType.RdmaShuffleManagerHello

  override protected def getLengthInSegments(segmentSize: Int): Array[Int] = {
    val serializedLength = rdmaShuffleManagerId.serializedLength + 4
    require(serializedLength <= segmentSize, "RdmaBuffer RPC segment size is too small")

    Array.fill(1) { serializedLength }
  }

  override protected def writeSegments(outs: Iterator[(DataOutputStream, Int)]): Unit = {
    val out = outs.next()._1
    rdmaShuffleManagerId.write(out)
    out.writeInt(channelPort)
  }

  override protected def read(in: DataInputStream): Unit = {
    rdmaShuffleManagerId = RdmaShuffleManagerId(in)
    channelPort = in.readInt()
  }
}

object RdmaShuffleManagerHelloRpcMsg {
  def apply(in: DataInputStream): RdmaRpcMsg = {
    val obj = new RdmaShuffleManagerHelloRpcMsg()
    obj.read(in)
    obj
  }
}

class RdmaAnnounceRdmaShuffleManagersRpcMsg(var rdmaShuffleManagerIds: Seq[RdmaShuffleManagerId])
    extends RdmaRpcMsg {
  private def this() = this(null)  // For deserialization only

  override protected def msgType: RdmaRpcMsgType = RdmaRpcMsgType.AnnounceRdmaShuffleManagers

  override protected def getLengthInSegments(segmentSize: Int): Array[Int] = {
    var segmentSizes = new ArrayBuffer[Int]
    for (rdmaShuffleManagerId <- rdmaShuffleManagerIds) {
      val serializedLength = rdmaShuffleManagerId.serializedLength

      if (segmentSizes.nonEmpty && (segmentSizes.last + serializedLength <= segmentSize)) {
        segmentSizes.update(segmentSizes.length - 1, segmentSizes.last + serializedLength)
      } else {
        segmentSizes += serializedLength
      }
    }

    segmentSizes.toArray
  }

  override protected def writeSegments(outs: Iterator[(DataOutputStream, Int)]): Unit = {
    var curOut: (DataOutputStream, Int) = null
    var curSegmentLength = 0

    def nextOut() {
      curOut = outs.next()
      curSegmentLength = 0
    }

    nextOut()
    for (rdmaShuffleManagerId <- rdmaShuffleManagerIds) {
      val serializedLength = rdmaShuffleManagerId.serializedLength

      if (curSegmentLength + serializedLength > curOut._2) {
        nextOut()
      }
      curSegmentLength += serializedLength
      rdmaShuffleManagerId.write(curOut._1)
    }
  }

  override protected def read(in: DataInputStream): Unit = {
    val tmpRdmaShuffleManagerIds = new ArrayBuffer[RdmaShuffleManagerId]
    scala.util.control.Exception.ignoring(classOf[EOFException]) {
      while (true) {
        tmpRdmaShuffleManagerIds += RdmaShuffleManagerId(in)
      }
    }
    rdmaShuffleManagerIds = tmpRdmaShuffleManagerIds
  }
}

object RdmaAnnounceRdmaShuffleManagersRpcMsg {
  def apply(in: DataInputStream): RdmaRpcMsg = {
    val obj = new RdmaAnnounceRdmaShuffleManagersRpcMsg()
    obj.read(in)
    obj
  }
}

/**
 * Prefetches blocks for a given reduceId and mapIds
 * @param shuffleId
 * @param reduceId
 * @param mapIds
 */
case class RdmaWriteBlocks(var callBackId: Int,
                      var nBlocks: Int,
                      var resultBuffer: (Long, Int),
                      var rdmaShuffleManagerId: RdmaShuffleManagerId,
                      var blocks: Seq[ShuffleBlockId])
  extends RdmaRpcMsg {
  private def this() = this(0, 0, null, null, null)  // For deserialization only

  override protected def msgType: RdmaRpcMsgType = RdmaRpcMsgType.RdmaPrefetchRPCMessage

  override protected def getLengthInSegments(segmentSize: Int): Array[Int] = {
    val msgSize = 20 + rdmaShuffleManagerId.serializedLength + 12 * blocks.size
    require(msgSize <= segmentSize,
      s"Message size: $msgSize is greater than segment size $segmentSize")
    Array.fill(1) { msgSize }
  }

  override protected def writeSegments(outs: Iterator[(DataOutputStream, Int)]): Unit = {
    var curOut: (DataOutputStream, Int) = null

    curOut = outs.next()
    curOut._1.writeInt(callBackId)
    curOut._1.writeInt(nBlocks)
    curOut._1.writeLong(resultBuffer._1)
    curOut._1.writeInt(resultBuffer._2)
    rdmaShuffleManagerId.write(curOut._1)

    for (block <- blocks) {
      curOut._1.writeInt(block.shuffleId)
      curOut._1.writeInt(block.mapId)
      curOut._1.writeInt(block.reduceId)
    }
  }

  override protected def read(in: DataInputStream): Unit = {
    callBackId = in.readInt()
    nBlocks = in.readInt()
    val address = in.readLong()
    val key = in.readInt()
    resultBuffer = (address, key)
    rdmaShuffleManagerId = RdmaShuffleManagerId(in)
    val tmpBlocks = new ArrayBuffer[ShuffleBlockId]
    scala.util.control.Exception.ignoring(classOf[EOFException]) {
      while (true) {
        val shuffleId = in.readInt()
        val mapId = in.readInt()
        val reduceId = in.readInt()
        tmpBlocks += ShuffleBlockId(shuffleId, mapId, reduceId)
      }
    }
    blocks = tmpBlocks
  }
}

object RdmaWriteBlocks {
  def apply(in: DataInputStream): RdmaRpcMsg = {
    val obj = new RdmaWriteBlocks()
    obj.read(in)
    obj
  }
}
