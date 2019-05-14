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

package org.apache.spark.shuffle.ucx.rpc;

import org.apache.spark.shuffle.SerializableBlockManagerId;
import org.apache.spark.storage.BlockManagerId;
import org.apache.spark.unsafe.Platform;
import org.apache.spark.util.ByteBufferInputStream;
import org.apache.spark.util.ByteBufferOutputStream;

import java.io.*;
import java.nio.ByteBuffer;

/**
 * RPC message sent by executor to driver to introduce it's worker address.
 * Driver broadcast this message to other executors, so they can create ALL-to-ALL connections.
 */
public class HelloMessage {

  public static final int tag = 0;

  private ByteBuffer workerAddress;

  public ByteBuffer getWorkerAddress() {
    return workerAddress;
  }

  public BlockManagerId getBlockManagerId() {
    return blockManagerId;
  }

  private BlockManagerId blockManagerId;

  public HelloMessage(BlockManagerId blockManagerId, ByteBuffer workerAddress) {
    this.blockManagerId = blockManagerId;
    this.workerAddress = workerAddress;
    workerAddress.clear();
  }

  public static HelloMessage deserialize(ByteBuffer msg) {
    BlockManagerId blockManagerId = SerializableBlockManagerId.apply(
      new DataInputStream(new ByteBufferInputStream(msg))).toBlockManagerId();
    int workerAddressSize = msg.getInt();
    ByteBuffer workerAddress = Platform.allocateDirectBuffer(workerAddressSize);
    for (int i = 0; i < workerAddressSize; i++) {
      workerAddress.put(msg.get());
    }
    return new HelloMessage(blockManagerId, workerAddress);
  }

  public ByteBuffer serialize() throws IOException {
    ByteBufferOutputStream bout = new ByteBufferOutputStream(4096);
    DataOutputStream dout = new DataOutputStream(bout);
    new SerializableBlockManagerId(blockManagerId).write(dout);
    dout.close();
    ByteBuffer byteOutputBuffer = bout.toByteBuffer();
    ByteBuffer msg = Platform.allocateDirectBuffer(bout.getCount() + workerAddress.capacity() + 4);
    ByteBuffer serializedBlockManager = ByteBuffer.wrap(byteOutputBuffer.array(), 0,
      bout.getCount());
    msg.put(serializedBlockManager);
    msg.putInt(workerAddress.capacity());
    msg.put(workerAddress);
    msg.clear();
    return msg;
  }

}
