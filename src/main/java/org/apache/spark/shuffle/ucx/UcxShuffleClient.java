package org.apache.spark.shuffle.ucx;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.Snapshot;
import com.codahale.metrics.UniformReservoir;
import org.apache.spark.SparkEnv;
import org.apache.spark.network.buffer.ManagedBuffer;
import org.apache.spark.network.buffer.NioManagedBuffer;
import org.apache.spark.network.shuffle.BlockFetchingListener;
import org.apache.spark.network.shuffle.DownloadFileManager;
import org.apache.spark.network.shuffle.ShuffleClient;
import org.apache.spark.shuffle.UcxShuffleManager;
import org.apache.spark.storage.BlockId$;
import org.apache.spark.storage.BlockManagerId;
import org.apache.spark.storage.ShuffleBlockId;
import org.apache.spark.util.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.openucx.jucx.UcxCallback;
import org.openucx.jucx.UcxException;
import org.openucx.jucx.UcxRequest;
import org.openucx.jucx.ucp.UcpEndpoint;
import org.openucx.jucx.ucp.UcpRemoteKey;
import scala.Option;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicReferenceArray;

public class UcxShuffleClient extends ShuffleClient {
  private final MemoryPool mempool;
  private final Histogram histogram = new Histogram(new UniformReservoir());

  protected static final Logger logger = LoggerFactory.getLogger(UcxShuffleClient.class);
  private int shuffleId;
  private UcxShuffleManager ucxShuffleManager;

  AtomicReferenceArray<UcpRemoteKey> dataRkeyArray;

  public UcxShuffleClient(int shuffleId) {
    this.shuffleId = shuffleId;
    this.ucxShuffleManager = (UcxShuffleManager) SparkEnv.get().shuffleManager();
    this.mempool = ucxShuffleManager.getUcxNode().getMemoryPool();
    this.dataRkeyArray = ucxShuffleManager.dataRkeyCache().get(shuffleId).get();
  }

  @Override
  public void fetchBlocks(String host, int port, String execId,
                          String[] blockIds, BlockFetchingListener listener,
                          DownloadFileManager downloadFileManager) {
    BlockManagerId blockManagerId = BlockManagerId.apply(execId, host, port, Option.apply(null));
    UcpEndpoint endpoint = ucxShuffleManager.getUcxNode().getConnection(blockManagerId).endpoint;
    ByteBuffer metadata = ucxShuffleManager.driverMetadaBuffer().get(shuffleId).get().data();

    AtomicReferenceArray<UcpRemoteKey> offsetRkeyArray = ucxShuffleManager.offsetRkeyCache()
      .get(shuffleId).get();

    for (String block : blockIds) {
      ShuffleBlockId blockId = (ShuffleBlockId) BlockId$.MODULE$.apply(block);

      // Get block offset
      int mapIdBlock = blockId.mapId() *
        (int) ucxShuffleManager.ucxShuffleConf().metadataBlockSize();
      int offsetWithinBlock = 0;
      long offsetAdress = metadata.getLong(mapIdBlock + offsetWithinBlock);
      offsetWithinBlock += 8;
      long dataAddress = metadata.getLong(mapIdBlock + offsetWithinBlock);
      offsetWithinBlock += 8;

      if (offsetRkeyArray.get(blockId.mapId()) == null ||
        dataRkeyArray.get(blockId.mapId()) == null) {
        int offsetRKeySize = metadata.getInt(mapIdBlock + offsetWithinBlock);
        offsetWithinBlock += 4;
        int dataRkeySize = metadata.getInt(mapIdBlock + offsetWithinBlock);
        offsetWithinBlock += 4;


        if (offsetRKeySize <= 0 || dataRkeySize <= 0) {
          logger.error("Metadata: {}", metadata.asCharBuffer().toString());
          throw new UcxException("Wrong rkey size");
        }
        final ByteBuffer rkeyCopy = metadata.slice();
        rkeyCopy.position(mapIdBlock + offsetWithinBlock)
          .limit(mapIdBlock + offsetWithinBlock + offsetRKeySize);


        offsetWithinBlock += offsetRKeySize;

        UcpRemoteKey offsetMemory = endpoint.unpackRemoteKey(rkeyCopy);

        if (!offsetRkeyArray.compareAndSet(blockId.mapId(), null, offsetMemory)) {
          offsetMemory.close();
        } else {
          logger.trace("Unpacked offset rkey for mapID{} BM {} by endpoint {}",
            blockId.mapId(), blockManagerId, endpoint.getNativeId());
        }

        if (dataRkeyArray.get(blockId.mapId()) == null) {
          rkeyCopy.position(mapIdBlock + offsetWithinBlock)
            .limit(mapIdBlock + offsetWithinBlock + dataRkeySize);

          UcpRemoteKey dataMemory = endpoint.unpackRemoteKey(rkeyCopy);
          if (!dataRkeyArray.compareAndSet(blockId.mapId(), null, dataMemory)) {
            dataMemory.close();
          }
        }
      }
      RegisteredMemory offsetMemory = mempool.get(16);
      ByteBuffer resultOffset = offsetMemory.getBuffer();
      endpoint.getNonBlocking(offsetAdress + blockId.reduceId() * 8L,
        offsetRkeyArray.get(blockId.mapId()), resultOffset, new UcxCallback() {
          long startTime = System.currentTimeMillis();

          @Override
          public void onError(int ucsStatus, String errorMsg) {
            logger.error("Failed to fetch offset for block {}, to bm {} by ep {}: {}." +
                "At address: {}.", blockId, blockManagerId, endpoint.getNativeId(),
              errorMsg, offsetAdress + blockId.reduceId() * 8L);
            System.exit(-1);
          }

          @Override
          public void onSuccess(UcxRequest request) {
            long blockOffset = resultOffset.getLong(0);
            long blockLength = resultOffset.getLong(8) - blockOffset;
            logger.trace("Got data offset from address {} for block {} took: {}. " +
                "Will read block of size {} b from data file",
              offsetAdress + blockId.reduceId() * 8L,
              blockId, Utils.getUsedTimeMs(startTime), blockLength);
            mempool.put(offsetMemory);

            RegisteredMemory blockMemory =  mempool.get((int) blockLength);
            ByteBuffer blockBuffer = blockMemory.getBuffer();
            UcpRemoteKey blockKey = dataRkeyArray.get(blockId.mapId());
            endpoint.getNonBlocking(dataAddress + blockOffset, blockKey,
              blockBuffer, new UcxCallback() {
                long startBlockFetch = System.currentTimeMillis();

                @Override
                public void onError(int ucsStatus, String errorMsg) {
                  logger.error("Failed to fetch block {} of size {}: {}",
                    block, blockLength, errorMsg);
                  System.exit(-1);
                }

                @Override
                public void onSuccess(UcxRequest request) {
                  histogram.update(System.currentTimeMillis() - startTime);
                  logger.trace("Fetched block {} of size {} in {}. Total ofset + data read {}",
                    blockId, blockLength, Utils.getUsedTimeMs(startBlockFetch),
                    Utils.getUsedTimeMs(startTime));
                  listener.onBlockFetchSuccess(block, new NioManagedBuffer(blockBuffer) {
                    @Override
                    public ManagedBuffer release() {
                      mempool.put(blockMemory);
                      return this;
                    }
                  });
                }
              });
          }
        });
    }
  }

  @Override
  public void close() {
    Snapshot histSnapshot = histogram.getSnapshot();
    logger.info("Total blocks ({}) fetch time statistics: min: {}ms, " +
        "mean: {}ms, median: {}ms, 95%: {}ms, max: {}ms",
      histogram.getCount(), histSnapshot.getMin(), histSnapshot.getMean(),
      histSnapshot.getMedian(), histSnapshot.get95thPercentile(), histSnapshot.getMax());
  }
}
