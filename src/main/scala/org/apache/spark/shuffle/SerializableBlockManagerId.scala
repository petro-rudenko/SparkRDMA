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

import java.io.{DataInputStream, DataOutputStream}
import java.nio.charset.Charset

import org.apache.spark.storage.BlockManagerId

class SerializableBlockManagerId private (executorId__ : String, host__ : String, port__ : Int) {
  private val blockManagerId = BlockManagerId(executorId__, host__, port__)


  def this(blockManagerId: BlockManagerId) {
    this(blockManagerId.executorId, blockManagerId.host, blockManagerId.port)
  }

  def toBlockManagerId: BlockManagerId = blockManagerId

  def write(out: DataOutputStream) {
    val executorIdInUtf: Array[Byte] =
    blockManagerId.executorId.getBytes(Charset.forName("UTF-8"))
    val blockManagerHostNameInUtf: Array[Byte] =
    blockManagerId.host.getBytes(Charset.forName("UTF-8"))
    out.writeShort(executorIdInUtf.length)
    out.write(executorIdInUtf)
    out.writeShort(blockManagerHostNameInUtf.length)
    out.write(blockManagerHostNameInUtf)
    out.writeInt(blockManagerId.port)
  }
}

object SerializableBlockManagerId {
  def apply(in: DataInputStream): SerializableBlockManagerId = {
    val executorIdInUtf = new Array[Byte](in.readShort())
    in.read(executorIdInUtf)
    val blockManagerHostNameInUtf = new Array[Byte](in.readShort())
    in.read(blockManagerHostNameInUtf)
    new SerializableBlockManagerId(new String(executorIdInUtf, "UTF-8"),
      new String(blockManagerHostNameInUtf, "UTF-8"), in.readInt())
  }
}
