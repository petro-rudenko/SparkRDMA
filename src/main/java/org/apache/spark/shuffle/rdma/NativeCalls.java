package org.apache.spark.shuffle.rdma;

public class NativeCalls {
  static
  {
    System.loadLibrary("spark-rdma-native");
  }

  native void writeIndexFileAndCommit(int shuffleId, int mapId, long[] lengths, String dataTmp);
}
