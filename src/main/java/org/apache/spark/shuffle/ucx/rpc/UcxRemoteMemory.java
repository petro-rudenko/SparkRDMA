package org.apache.spark.shuffle.ucx.rpc;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;

/**
 * Utility class to serialize / deserialize remote address and rkeyBuffer
 */
public class UcxRemoteMemory implements java.io.Serializable {
  private static final long serialVersionUID = -2831273345165209113L;

  private long address;

  private ByteBuffer rKeyBuffer;

  public UcxRemoteMemory(long address, ByteBuffer rKeyBuffer) {
    this.address = address;
    this.rKeyBuffer = rKeyBuffer;
  }

  private void writeObject(ObjectOutputStream out) throws IOException {
    out.writeLong(address);
    out.writeInt(rKeyBuffer.limit());
    byte[] copy = new byte[rKeyBuffer.limit()];
    rKeyBuffer.clear();
    rKeyBuffer.get(copy);
    out.write(copy);
  }

  private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
    address = in.readLong();

    //read buffer data and wrap with ByteBuffer
    int bufferSize = in.readInt();
    byte[] buffer = new byte[bufferSize];
    in.read(buffer, 0, bufferSize);
    this.rKeyBuffer = ByteBuffer.allocateDirect(bufferSize).put(buffer);
    this.rKeyBuffer.clear();
  }

  public long getAddress() {
    return address;
  }

  public ByteBuffer getrKeyBuffer() {
    return rKeyBuffer;
  }

}
