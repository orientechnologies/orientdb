package com.orientechnologies.orient.core.tx;

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.orient.core.exception.ODatabaseException;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

public class OTransactionData {
  private OTransactionId               transactionId;
  private List<OTransactionDataChange> changes = new ArrayList<>();

  public OTransactionData(OTransactionId transactionId) {
    this.transactionId = transactionId;
  }

  public static OTransactionData read(DataInput dataInput) throws IOException {
    OTransactionId transactionId = OTransactionId.read(dataInput);
    int entries = dataInput.readInt();
    OTransactionData data = new OTransactionData(transactionId);
    while (entries-- > 0) {
      data.changes.add(OTransactionDataChange.deserialize(dataInput));
    }
    return data;
  }

  public void addRecord(byte[] record) {
    try {
      changes.add(OTransactionDataChange.deserialize(new DataInputStream(new ByteArrayInputStream(record))));
    } catch (IOException e) {
      throw OException.wrapException(new ODatabaseException("error reading transaction data change record"), e);
    }
  }

  public void addChange(OTransactionDataChange change) {
    this.changes.add(change);
  }

  public OTransactionId getTransactionId() {
    return transactionId;
  }

  public List<OTransactionDataChange> getChanges() {
    return changes;
  }

  public void write(DataOutput output) throws IOException {
    transactionId.write(output);
    output.writeInt(changes.size());
    for (OTransactionDataChange change : changes) {
      change.serialize(output);
    }

  }
}
