package com.orientechnologies.orient.core.tx;

import java.io.*;
import java.util.concurrent.CountDownLatch;

public class OTxMetadataHolderImpl implements OTxMetadataHolder {
  private final CountDownLatch request;
  private final byte[]         status;
  private final OTransactionId id;

  public OTxMetadataHolderImpl(CountDownLatch request, OTransactionId id, byte[] status) {
    this.request = request;
    this.id = id;
    this.status = status;
  }

  @Override
  public byte[] metadata() {
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    DataOutput output = new DataOutputStream(outputStream);
    try {
      id.write(output);
      output.writeInt(status.length);
      output.write(status, 0, status.length);
    } catch (IOException e) {
      e.printStackTrace();
    }
    return outputStream.toByteArray();
  }

  public static OTxMetadataHolder read(byte[] data) {
    ByteArrayInputStream inputStream = new ByteArrayInputStream(data);
    DataInput input = new DataInputStream(inputStream);
    OTransactionId txId = null;
    try {
      txId = OTransactionId.read(input);
      int size = input.readInt();
      byte[] stauts = new byte[size];
      input.readFully(stauts);
      return new OTxMetadataHolderImpl(new CountDownLatch(0), txId, stauts);
    } catch (IOException e) {
      e.printStackTrace();
    }

    return null;
  }

  @Override
  public void notifyMetadataRead() {
    request.countDown();
  }

  public OTransactionId getId() {
    return id;
  }
}
