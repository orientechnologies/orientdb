package com.orientechnologies.orient.core.storage.impl.local;

import java.util.ArrayList;
import java.util.List;

public class OTransactionData {
  private byte[]       transactionId;
  private List<byte[]> records = new ArrayList<>();

  public OTransactionData(byte[] transactionId) {
    this.transactionId = transactionId;
  }

  public void addRecord(byte[] record) {
    records.add(record);
  }

}
