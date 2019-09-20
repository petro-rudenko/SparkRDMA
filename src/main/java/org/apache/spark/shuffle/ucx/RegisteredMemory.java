package org.apache.spark.shuffle.ucx;

import org.openucx.jucx.ucp.UcpMemory;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;

public class RegisteredMemory {
  private AtomicInteger refcount;
  private UcpMemory memory;
  private ByteBuffer buffer;

  public RegisteredMemory(AtomicInteger refcount, UcpMemory memory, ByteBuffer buffer) {
    this.refcount = refcount;
    this.memory = memory;
    this.buffer = buffer;
  }

  public ByteBuffer getBuffer() {
    return buffer;
  }

  public AtomicInteger getRefCount() {
    return refcount;
  }

  public void deregisterNativeMemory() {
    if (memory.getNativeId() != null) {
      memory.deregister();
    }
  }
}
