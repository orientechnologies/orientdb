package com.orientechnologies.orient.core.transaction;

import com.orientechnologies.orient.core.tx.OTransactionSequenceStatus;
import com.orientechnologies.orient.core.tx.OTxMetadataHolder;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;

public class OTxMetadataHolderLocal implements OTxMetadataHolder {

  private final OTransactionSequenceStatus status;
  private final OTransactionId id;

  public OTxMetadataHolderLocal(OTransactionId id, OTransactionSequenceStatus status) {
    this.id = id;
    this.status = status;
  }

  @Override
  public byte[] metadata() {
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    DataOutput output = new DataOutputStream(outputStream);
    try {
      id.writeDisk(output);
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
      final OTransactionId txId = OTransactionId.readDisk(input);
      int size = input.readInt();
      byte[] status = new byte[size];
      input.readFully(status);
      return new OTxMetadataHolderLocal(txId, OTransactionSequenceStatus.read(status));
    } catch (IOException e) {
      e.printStackTrace();
    }

    return null;
  }

  @Override
  public void notifyMetadataRead() {}

  @Override
  public OTransactionId getId() {
    return id;
  }

  @Override
  public OTransactionSequenceStatus getStatus() {
    return status;
  }
}
