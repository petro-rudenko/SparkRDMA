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


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class UcxNode implements Closeable {
  private final UcxShuffleConf conf;
  private final UcpContext context;
  private final MemoryPool memoryPool;
  private final UcpWorkerParams workerParams;
  private volatile UcpWorker globalWorker;
  private volatile UcpListener listener;
  private boolean closed = false;

  private Queue<UcxWorkerWrapper> workerPool = new ConcurrentLinkedDeque<>();
  private static final Logger logger = LoggerFactory.getLogger(UcxNode.class);

  private Thread listenerProgressThread;

  public UcxNode(UcxShuffleConf conf, boolean isDriver) {
    this.conf = conf;
    UcpParams params = new UcpParams().requestRmaFeature().requestWakeupFeature()
      .setMtWorkersShared(true)
      .setEstimatedNumEps(conf.getNumProcesses());
    workerParams = new UcpWorkerParams().requestThreadSafety();
    context = new UcpContext(params);
    memoryPool = new MemoryPool(context, conf);

    listenerProgressThread = new Thread() {
      @Override
      public void run() {
        globalWorker = context.newWorker(workerParams);
        BlockManagerId blockManagerId = SparkEnv.get().blockManager().blockManagerId();
        InetSocketAddress socketAddress;
        if (isDriver) {
          socketAddress = new InetSocketAddress(conf.driverHost(), conf.driverPort());
        } else {
          socketAddress = new InetSocketAddress(blockManagerId.host(), blockManagerId.port() + 7);
        }
        UcpListenerParams listenerParams = new UcpListenerParams().setSockAddr(socketAddress);
        listener = globalWorker.newListener(listenerParams);
        logger.info("Started UcxNode on {}", socketAddress);
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

  MemoryPool getMemoryPool() {
    return memoryPool;
  }

  public UcpContext getContext() {
    return context;
  }

  public UcxWorkerWrapper getWorker() {
    UcxWorkerWrapper result = workerPool.poll();
    if (result == null) {
      logger.warn("Creating new worker wrapper.");
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
        context.close();
        workerPool.forEach(UcxWorkerWrapper::close);
        workerPool.clear();
        closed = true;
      }
    }
  }
}
