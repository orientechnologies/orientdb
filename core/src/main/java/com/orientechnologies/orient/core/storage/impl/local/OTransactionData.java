package com.orientechnologies.orient.core.storage.impl.local;

import com.orientechnologies.orient.core.tx.OTransactionId;

import java.util.ArrayList;
import java.util.List;

public class OTransactionData {
  private OTransactionId transactionId;
  private List<byte[]>   changes = new ArrayList<>();

  public OTransactionData(OTransactionId transactionId) {
    this.transactionId = transactionId;
  }

  public void addRecord(byte[] record) {
    changes.add(record);
  }

  public OTransactionId getTransactionId() {
    return transactionId;
  }

  public List<byte[]> getChanges() {
    return changes;
  }
}
