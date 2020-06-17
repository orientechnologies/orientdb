package com.orientechnologies.orient.core.tx;

import com.orientechnologies.orient.core.serialization.serializer.record.binary.OVarIntSerializer;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Arrays;

public class OTransactionSequenceStatus {
  private final long[] status;

  public OTransactionSequenceStatus(long[] status) {
    this.status = status;
  }

  public byte[] store() throws IOException {
    ByteArrayOutputStream buffer = new ByteArrayOutputStream();
    DataOutput dataOutput = new DataOutputStream(buffer);
    OVarIntSerializer.write(dataOutput, this.status.length);
    for (int i = 0; i < this.status.length; i++) {
      OVarIntSerializer.write(dataOutput, this.status[i]);
    }
    return buffer.toByteArray();
  }

  public static OTransactionSequenceStatus read(byte[] data) throws IOException {
    DataInput dataInput = new DataInputStream(new ByteArrayInputStream(data));
    int len = OVarIntSerializer.readAsInt(dataInput);
    long[] newSequential = new long[len];
    for (int i = 0; i < len; i++) {
      newSequential[i] = OVarIntSerializer.readAsLong(dataInput);
    }
    return new OTransactionSequenceStatus(newSequential);
  }

  public void writeNetwork(DataOutput dataOutput) throws IOException {
    OVarIntSerializer.write(dataOutput, this.status.length);
    for (int i = 0; i < this.status.length; i++) {
      OVarIntSerializer.write(dataOutput, this.status[i]);
    }
  }

  public static OTransactionSequenceStatus readNetwork(DataInput dataInput) throws IOException {
    int len = OVarIntSerializer.readAsInt(dataInput);
    long[] newSequential = new long[len];
    for (int i = 0; i < len; i++) {
      newSequential[i] = OVarIntSerializer.readAsLong(dataInput);
    }
    return new OTransactionSequenceStatus(newSequential);
  }

  public long[] getStatus() {
    return status;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    OTransactionSequenceStatus that = (OTransactionSequenceStatus) o;
    return Arrays.equals(status, that.status);
  }

  @Override
  public int hashCode() {
    return Arrays.hashCode(status);
  }
}
