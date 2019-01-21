package org.apache.spark.shuffle.rdma;

import java.io.FileDescriptor;

public class SparkRdmaNativeInterface {
  static
  {
    System.loadLibrary("spark-rdma-native");
  }

  native long map(FileDescriptor fd, long offset, long length);
  native void unmap(long addr, long length);
  native void writeBlocks(int shuffleId, int[] mapIs, int[] reduceIds, long qp,
                          long rAddress, int rKey, int callbackId);
  native void unregisterShuffle(int shuffleId);
}
