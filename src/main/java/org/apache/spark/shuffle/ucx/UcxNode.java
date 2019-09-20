package org.apache.spark.shuffle.ucx;

import org.apache.spark.SparkEnv;
import org.apache.spark.shuffle.UcxShuffleConf;
import org.apache.spark.shuffle.UcxWorkerWrapper;
import org.apache.spark.storage.BlockManagerId;
import org.openucx.jucx.ucp.*;

import java.io.Closeable;
import java.net.InetSocketAddress;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicInteger;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class UcxNode implements Closeable {
  private final UcxShuffleConf conf;
  private final UcpContext context;
  private final MemoryPool memoryPool;
  private final UcpWorkerParams workerParams = new UcpWorkerParams().requestThreadSafety();
  private volatile UcpWorker globalWorker;
  private volatile UcpListener listener;
  private boolean closed = false;

  private final Queue<UcxWorkerWrapper> workerPool = new ConcurrentLinkedDeque<>();
  private static final Logger logger = LoggerFactory.getLogger(UcxNode.class);
  private static final AtomicInteger numWorkers = new AtomicInteger(0);

  private Thread listenerProgressThread;

  public UcxNode(UcxShuffleConf conf, boolean isDriver) {
    this.conf = conf;
    UcpParams params = new UcpParams().requestRmaFeature().requestWakeupFeature()
      .setMtWorkersShared(true)
      .setEstimatedNumEps(conf.coresPerProcess() * conf.getNumProcesses());
    context = new UcpContext(params);
    memoryPool = new MemoryPool(context, conf);
    globalWorker = context.newWorker(workerParams);
    InetSocketAddress socketAddress;
    if (isDriver) {
      socketAddress = new InetSocketAddress(conf.driverHost(), conf.driverPort());
    } else {
      BlockManagerId blockManagerId = SparkEnv.get().blockManager().blockManagerId();
      socketAddress = new InetSocketAddress(blockManagerId.host(), blockManagerId.port() + 7);
    }
    UcpListenerParams listenerParams = new UcpListenerParams().setSockAddr(socketAddress);
    listener = globalWorker.newListener(listenerParams);
    logger.info("Started UcxNode on {}", socketAddress);

    listenerProgressThread = new Thread() {
      @Override
      public void run() {
        while (!isInterrupted()) {
          try {
            if (globalWorker.progress() == 0) {
              globalWorker.waitForEvents();
            }
          } catch (Exception ex) {
            logger.error("Fail during progress. Stoping...");
            close();
          }
        }
      }
    };

    listenerProgressThread.setName("Listener progress thread.");
    listenerProgressThread.setDaemon(true);
    listenerProgressThread.start();

    if (!isDriver) {
      memoryPool.preAlocate();
      logger.info("Creating {} workers", conf.coresPerProcess());
      for (int i = 0; i < conf.coresPerProcess(); i++) {
        UcpWorker worker = context.newWorker(workerParams);
        UcxWorkerWrapper workerWrapper = new UcxWorkerWrapper(worker, conf);
        workerPool.add(workerWrapper);
      }
    }
  }

  public MemoryPool getMemoryPool() {
    return memoryPool;
  }

  public UcpContext getContext() {
    return context;
  }

  public UcxWorkerWrapper getWorker() {
    UcxWorkerWrapper result = workerPool.poll();
    if (result == null) {
      logger.warn("Creating new worker wrapper: {}.", numWorkers.incrementAndGet());
      UcpWorker worker = context.newWorker(workerParams);
      result = new UcxWorkerWrapper(worker, conf);
    }
    return result;
  }

  public void putWorker(UcxWorkerWrapper worker) {
    workerPool.add(worker);
  }

  @Override
  public void close() {
    synchronized (this) {
      if (!closed) {
        logger.info("Stopping UcxNode");
        listenerProgressThread.interrupt();
        globalWorker.signal();
        memoryPool.close();
        listener.close();
        globalWorker.close();
        workerPool.forEach(UcxWorkerWrapper::close);
        workerPool.clear();
        context.close();
        closed = true;
      }
    }
  }
}
