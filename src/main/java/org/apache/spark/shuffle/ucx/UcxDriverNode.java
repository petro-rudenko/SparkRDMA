package org.apache.spark.shuffle.ucx;

import org.apache.spark.shuffle.UcxShuffleConf;
import org.apache.spark.shuffle.ucx.rpc.HelloMessage;
import org.apache.spark.storage.BlockManagerId;
import org.openucx.jucx.UcxCallback;
import org.openucx.jucx.UcxRequest;
import org.openucx.jucx.ucp.UcpEndpoint;
import org.openucx.jucx.ucp.UcpEndpointParams;
import org.openucx.jucx.ucp.UcpListener;
import org.openucx.jucx.ucp.UcpListenerParams;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;

public class UcxDriverNode extends UcxNode {

  private final UcxCallback connectionHandler = new UcxCallback() {
    @Override
    public void onSuccess(UcxRequest request) {
      ByteBuffer metadataBuffer = requestToMetadataBuffer.remove(request);
      HelloMessage msg = HelloMessage.deserialize(metadataBuffer);
      logger.info("Received message from executor {}, connecting to it.",
        msg.getBlockManagerId());
      connectToWorker(msg.getBlockManagerId(), msg.getWorkerAddress());
      recvNewMetadataMessage(workers[0], connectionHandler);
    }
  };

  public UcxDriverNode(UcxShuffleConf conf) {
    super(conf, true);
  }

  private void announceNewWorker(BlockManagerId blockManagerId, ByteBuffer workerAddress) {
    connections.forEach((bm, entry) -> {
      logger.info("Announcing blockmanagerId: {} to {}", blockManagerId, bm);
      HelloMessage msg = new HelloMessage(blockManagerId, workerAddress);
      try {
        entry.endpoint.sendTaggedNonBlocking(msg.serialize(), HelloMessage.tag,
          new UcxCallback() {
            @Override
            public void onSuccess(UcxRequest request) {
              logger.info("Sent message to announce bm {} to {}", blockManagerId, bm);
            }
          });
      } catch (IOException e) {
        e.printStackTrace();
      }
    });
  }

  private void introduceOtherWorkers(BlockManagerId blockManagerId) {
    EndpointAndWorkerAddress currentWorker = connections.get(blockManagerId);
    connections.forEach((bm, entry) -> {
      if (bm != blockManagerId) {
        logger.info("Introducing {} to worker {}", bm, blockManagerId);
        HelloMessage msg = new HelloMessage(bm, entry.workerAddress);
        try {
          currentWorker.endpoint.sendTaggedNonBlocking(msg.serialize(), HelloMessage.tag,
            new UcxCallback() {
              @Override
              public void onSuccess(UcxRequest request) {
                logger.info("Sent message to introduce executor {} to {}", bm, blockManagerId);
              }
            });
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
    });
  }

  public void connectToWorker(BlockManagerId blockManagerId, ByteBuffer workerAddress) {
    connections.computeIfAbsent(blockManagerId, (bm) -> {
      UcpEndpointParams params = new UcpEndpointParams().setUcpAddress(workerAddress)
        .setPeerErrorHadnlingMode();
      announceNewWorker(blockManagerId, workerAddress);
      UcpEndpoint newEndpoint = workers[0].newEndpoint(params);
      return new EndpointAndWorkerAddress(newEndpoint, workerAddress);
    });
    introduceOtherWorkers(blockManagerId);
  }


  public void startDriverListener() {
    InetSocketAddress socketAddress =
      new InetSocketAddress(conf.driverHost(), conf.driverPort());
    UcpListenerParams listenerParams = new UcpListenerParams().setSockAddr(socketAddress);
    workers[0].signal();
    listener = new UcpListener(workers[0], listenerParams);
    recvNewMetadataMessage(workers[0], connectionHandler);
  }


}
