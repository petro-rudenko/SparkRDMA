package org.apache.spark.shuffle.ucx;

import org.apache.spark.shuffle.UcxShuffleConf;
import org.apache.spark.shuffle.ucx.rpc.HelloMessage;
import org.apache.spark.storage.BlockManagerId;
import org.apache.spark.unsafe.Platform;
import org.openucx.jucx.UcxCallback;
import org.openucx.jucx.UcxRequest;
import org.openucx.jucx.ucp.*;

import java.io.Closeable;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class UcxNode implements Closeable {
  protected UcpContext context;
  protected UcpWorker[] workers;
  protected UcpListener listener;
  protected UcxShuffleConf conf;
  private MemoryPool memoryPool;

  public class EndpointAndWorkerAddress {
    public UcpEndpoint endpoint;
    public ByteBuffer workerAddress;

    public EndpointAndWorkerAddress(UcpEndpoint endpoint, ByteBuffer workerAddress) {
      this.endpoint = endpoint;
      this.workerAddress = workerAddress;
    }
  }

  protected final HashMap<BlockManagerId, EndpointAndWorkerAddress> connections =
    new HashMap<>();

  protected final ConcurrentHashMap<UcxRequest, ByteBuffer> requestToMetadataBuffer =
    new ConcurrentHashMap<>();

  private Thread[] progressThreads;
  protected static final Logger logger = LoggerFactory.getLogger(UcxNode.class);


  public UcxNode(UcxShuffleConf conf, boolean isDriver) {
    this.conf = conf;
    UcpParams params = new UcpParams().requestRmaFeature()
      .requestTagFeature().requestWakeupFeature().setMtWorkersShared(true)
      .setEstimatedNumEps(conf.getInt("spark.executor.instances", 1));
    int[] cpus = conf.cpus();
    int numWorkers = (isDriver) ? 1 : conf.numWorkers();
    context = new UcpContext(params);
    workers = new UcpWorker[numWorkers];
    progressThreads = new Thread[numWorkers];
    logger.info("Creating {} workers", numWorkers);
    for (int i = 0; i < numWorkers; i++) {
      UcpWorkerParams workerParams = new UcpWorkerParams().requestThreadSafety()
        .requestWakeupRMA().setCpu(cpus[i % cpus.length]);
      final UcpWorker worker = new UcpWorker(context, workerParams);
      workers[i] = worker;
      progressThreads[i] = new Thread() {
        @Override
        public void run() {
          while (!isInterrupted()) {
            try {
              if (worker.progress() == 0) {
                worker.waitForEvents();
              }
            } catch (Exception ex) {
              logger.error("Fail during progress. Stoping...");
              close();
            }
          }
        }
      };
      progressThreads[i].setDaemon(true);
      progressThreads[i].setName("JUCX progress thread " + i);
      progressThreads[i].start();
    }

    memoryPool = new MemoryPool(context);
  }

  protected void recvNewMetadataMessage(UcpWorker worker, UcxCallback callback) {
    ByteBuffer metadataBuffer = Platform.allocateDirectBuffer(conf.metadataBufferSize());
    UcxRequest recv = worker.recvTaggedNonBlocking(metadataBuffer,
      HelloMessage.tag, 0, callback);
    requestToMetadataBuffer.put(recv, metadataBuffer);
  }

  public EndpointAndWorkerAddress getConnection(BlockManagerId blockManagerId) {
    EndpointAndWorkerAddress result = connections.get(blockManagerId);
    if (result == null) {
      synchronized (this) {
        while (result == null) {
          try {
            logger.warn("No connection to {}. Waiting...", blockManagerId);
            wait(conf.getTimeAsMs("spark.network.timeout", "10000"));
            result = connections.get(blockManagerId);
          } catch (InterruptedException e) {
            e.printStackTrace();
            break;
          }
        }
      }
    }
    return result;
  }

  public MemoryPool getMemoryPool() {
    return memoryPool;
  }

  public UcpContext getContext() {
    return context;
  }

  @Override
  public void close() {
    synchronized (this) {
      if (memoryPool != null) {
        memoryPool.close();
      }
      for (Thread progressThread: progressThreads) {
        if (progressThread != null) {
          progressThread.interrupt();
          try {
            progressThread.join(1000);
          } catch (InterruptedException e) {
            e.printStackTrace();
          }
        }
      }
      if (listener != null) {
        listener.close();
        listener = null;
      }
      for (UcpWorker worker: workers) {
        if (worker != null) {
          worker.signal();
          worker.close();
        }
      }
      if (context != null) {
        context.close();
        context = null;
      }
    }
  }
}
