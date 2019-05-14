package org.apache.spark.shuffle.ucx;

import org.apache.spark.shuffle.UcxShuffleConf;
import org.apache.spark.unsafe.Platform;
import org.openucx.jucx.ucp.UcpContext;
import org.openucx.jucx.ucp.UcpMemory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.io.Closeable;
import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class MemoryPool implements Closeable {
  private static final Logger logger = LoggerFactory.getLogger(MemoryPool.class);

  @Override
  public void close() {
    for (AllocatorStack stack: allocStackMap.values()) {
      stack.close();
      logger.info("Stack of size {}. " +
          "Total requests: {}, total allocations: {}, preAllocations: {}, deallocations: {}",
        stack.length, stack.totalRequests.get(), stack.totalAlloc.get(), stack.preAllocs.get(),
        stack.totalDealloc.get());
    }
    allocStackMap.clear();
  }

  private class AllocatorStack implements Closeable {
    private final AtomicInteger totalRequests = new AtomicInteger(0);
    private final AtomicInteger totalAlloc = new AtomicInteger(0);
    private final AtomicInteger totalDealloc = new AtomicInteger(0);
    private final AtomicInteger preAllocs = new AtomicInteger(0);
    private final ConcurrentLinkedDeque<RegisteredMemory> stack = new ConcurrentLinkedDeque<>();
    private final int length;
    private final AtomicLong idleBuffersSize = new AtomicLong(0);

    private AllocatorStack(int length) {
      this.length = length;
    }

    private RegisteredMemory get() {
      RegisteredMemory result = stack.pollFirst();
      if (result == null) {
        ByteBuffer buffer = Platform.allocateDirectBuffer(length);
        UcpMemory memory = context.registerMemory(buffer);
        result = new RegisteredMemory(new AtomicInteger(1), memory, buffer);
        totalAlloc.incrementAndGet();
      } else {
        result.getRefCount().incrementAndGet();
      }
      totalRequests.incrementAndGet();
      return result;
    }

    private void put(RegisteredMemory registeredMemory) {
      registeredMemory.getRefCount().decrementAndGet();
      stack.addLast(registeredMemory);
      idleBuffersSize.addAndGet(length);
    }

    private void preallocate(int numBuffers) {
      ByteBuffer buffer = Platform.allocateDirectBuffer(length * numBuffers);
      UcpMemory memory = context.registerMemory(buffer);
      AtomicInteger refCount = new AtomicInteger(numBuffers);
      for (int i = 0; i < numBuffers; i++) {
        buffer.position(i * length).limit(i * length + length);
        final ByteBuffer slice = buffer.slice();
        RegisteredMemory registeredMemory = new RegisteredMemory(refCount, memory, slice);
        put(registeredMemory);
      }
      preAllocs.incrementAndGet();
      totalAlloc.incrementAndGet();
    }

    @Override
    public void close() {
      while (!stack.isEmpty()) {
        RegisteredMemory memory = stack.pollFirst();
        if (memory.getMemory().getNativeId() != null) {
          memory.getMemory().deregister();
          totalDealloc.incrementAndGet();
        }
      }
    }
  }

  private final ConcurrentHashMap<Integer, AllocatorStack> allocStackMap =
    new ConcurrentHashMap<>();
  private final UcpContext context;
  private final UcxShuffleConf conf;

  MemoryPool(UcpContext context, UcxShuffleConf conf) {
    this.context = context;
    this.conf = conf;
  }

  private long roundUpToTheNextPowerOf2(long length) {
    // Round up length to the nearest power of two, or the minimum block size
    if (length < conf.minAllocationSize()) {
      length = conf.minAllocationSize();
    } else {
      length--;
      length |= length >> 1;
      length |= length >> 2;
      length |= length >> 4;
      length |= length >> 8;
      length |= length >> 16;
      length++;
    }
    return length;
  }

  RegisteredMemory get(int size) {
    long roundedSize = roundUpToTheNextPowerOf2(size);
    assert roundedSize < Integer.MAX_VALUE;
    AllocatorStack stack =
      allocStackMap.computeIfAbsent((int)roundedSize, AllocatorStack::new);
    RegisteredMemory result = stack.get();
    result.getBuffer().position(0).limit(size);
    return result;
  }

  void put(RegisteredMemory memory) {
    ByteBuffer buffer = memory.getBuffer();
    AllocatorStack allocatorStack = allocStackMap.get(buffer.capacity());
    if (allocatorStack != null) {
      buffer.clear();
      allocatorStack.put(memory);
    }
    // TODO: cleaning task.
  }

  void preAlocate() {
    conf.preallocateBuffers().forEach((size, numBuffers) -> {
      AllocatorStack stack = new AllocatorStack(size);
      allocStackMap.put(size, stack);
      stack.preallocate(numBuffers);
    });
  }

}
