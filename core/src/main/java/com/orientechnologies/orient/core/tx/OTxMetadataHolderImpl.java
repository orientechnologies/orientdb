package com.orientechnologies.orient.core.tx;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.concurrent.CountDownLatch;

public class OTxMetadataHolderImpl implements OTxMetadataHolder {
  private final CountDownLatch request;
  private final OTransactionSequenceStatus status;
  private final OTransactionId id;

  public OTxMetadataHolderImpl(
      CountDownLatch request, OTransactionId id, OTransactionSequenceStatus status) {
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
      byte[] status = this.status.store();
      output.writeInt(status.length);
      output.write(status, 0, status.length);
    } catch (IOException e) {
      e.printStackTrace();
    }
    return outputStream.toByteArray();
  }

  public static OTxMetadataHolder read(final byte[] data) {
    final ByteArrayInputStream inputStream = new ByteArrayInputStream(data);
    final DataInput input = new DataInputStream(inputStream);
    try {
      final OTransactionId txId = OTransactionId.read(input);
      int size = input.readInt();
      byte[] status = new byte[size];
      input.readFully(status);
      return new OTxMetadataHolderImpl(
          new CountDownLatch(0), txId, OTransactionSequenceStatus.read(status));
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

  @Override
  public OTransactionSequenceStatus getStatus() {
    return status;
  }
}
