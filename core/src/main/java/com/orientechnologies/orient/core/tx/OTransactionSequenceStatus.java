package com.orientechnologies.orient.core.tx;

import java.io.*;
import java.util.Arrays;

public class OTransactionSequenceStatus {
  private final long[] status;

  public OTransactionSequenceStatus(long[] status) {
    this.status = status;
  }

  public byte[] store() throws IOException {
    ByteArrayOutputStream buffer = new ByteArrayOutputStream();
    DataOutput dataOutput = new DataOutputStream(buffer);
    dataOutput.writeInt(this.status.length);
    for (int i = 0; i < this.status.length; i++) {
      dataOutput.writeLong(this.status[i]);
    }
    return buffer.toByteArray();
  }

  public static OTransactionSequenceStatus read(byte[] data) throws IOException {
    DataInput dataInput = new DataInputStream(new ByteArrayInputStream(data));
    int len = dataInput.readInt();
    long[] newSequential = new long[len];
    for (int i = 0; i < len; i++) {
      newSequential[i] = dataInput.readLong();
    }
    return new OTransactionSequenceStatus(newSequential);
  }

  public void writeNetwork(DataOutput dataOutput) throws IOException {
    dataOutput.writeInt(this.status.length);
    for (int i = 0; i < this.status.length; i++) {
      dataOutput.writeLong(this.status[i]);
    }
  }

  public static OTransactionSequenceStatus readNetwork(DataInput dataInput) throws IOException {
    int len = dataInput.readInt();
    long[] newSequential = new long[len];
    for (int i = 0; i < len; i++) {
      newSequential[i] = dataInput.readLong();
    }
    return new OTransactionSequenceStatus(newSequential);
  }

  public long[] getStatus() {
    return status;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;
    OTransactionSequenceStatus that = (OTransactionSequenceStatus) o;
    return Arrays.equals(status, that.status);
  }

  @Override
  public int hashCode() {
    return Arrays.hashCode(status);
  }
}
