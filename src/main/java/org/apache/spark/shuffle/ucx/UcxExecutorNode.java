package org.apache.spark.shuffle.ucx;

import org.apache.spark.SparkEnv;
import org.apache.spark.shuffle.UcxShuffleConf;
import org.apache.spark.shuffle.ucx.rpc.HelloMessage;
import org.apache.spark.storage.BlockManagerId;
import org.openucx.jucx.UcxCallback;
import org.openucx.jucx.UcxRequest;
import org.openucx.jucx.ucp.UcpEndpoint;
import org.openucx.jucx.ucp.UcpEndpointParams;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;

public class UcxExecutorNode extends UcxNode {
  UcpEndpoint driverConnection;
  private static AtomicInteger workerIdx = new AtomicInteger(0);

  UcxCallback connectionHandler = new UcxCallback() {
    @Override
    public void onSuccess(UcxRequest request) {
      ByteBuffer metadataBuffer = requestToMetadataBuffer.remove(request);
      HelloMessage msg = HelloMessage.deserialize(metadataBuffer);
      logger.info("Received message from driver. Connecting to executor {}",
        msg.getBlockManagerId());
      ByteBuffer workerAddress = msg.getWorkerAddress();
      connectToExecutor(msg.getBlockManagerId(), workerAddress);
      recvNewMetadataMessage(workers[0], connectionHandler);
    }
  };

  public UcxExecutorNode(UcxShuffleConf conf) {
    super(conf, false);
    try {
      getMemoryPool().preAlocate();
      connectToDriver();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  private void connectToExecutor(BlockManagerId blockManagerId, ByteBuffer workerAddres) {
    synchronized (this) {
      UcpEndpointParams endpointParams = new UcpEndpointParams().setUcpAddress(workerAddres)
        .setPeerErrorHadnlingMode();
      UcpEndpoint endpoint = workers[workerIdx.incrementAndGet() % workers.length]
        .newEndpoint(endpointParams);
      EndpointAndWorkerAddress prev =
        connections.put(blockManagerId, new EndpointAndWorkerAddress(endpoint, workerAddres));
      if (prev != null) {
        prev.endpoint.close();
      }
      notifyAll();
    }
  }

  public void connectToDriver() throws IOException {
    InetSocketAddress socketAddress =
      new InetSocketAddress(conf.driverHost(), conf.driverPort());
    UcpEndpointParams endpointParams = new UcpEndpointParams().setSocketAddress(socketAddress)
      .setPeerErrorHadnlingMode();
    driverConnection = workers[0].newEndpoint(endpointParams);
    BlockManagerId blockManagerId = SparkEnv.get().blockManager().blockManagerId();
    ByteBuffer workerAddress = workers[0].getAddress();
    ByteBuffer helloMessage = new HelloMessage(blockManagerId, workerAddress).serialize();
    logger.info("Connecting to driver at address: {}. Msg #{}", socketAddress,
      helloMessage.hashCode());
    helloMessage.clear();
    driverConnection.sendTaggedNonBlocking(helloMessage, HelloMessage.tag, null);
    recvNewMetadataMessage(workers[0], connectionHandler);
  }

  public UcpEndpoint getDriverEndpoint() {
    return driverConnection;
  }

}
