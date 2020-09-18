package com.orientechnologies.common.concur.lock;

import com.orientechnologies.orient.core.tx.OTransactionId;

public class Promise<T> {
  private final T key;
  private final Integer version;
  private final OTransactionId txId;

  public Promise(T key, Integer version, OTransactionId txId) {
    this.key = key;
    this.version = version;
    this.txId = txId;
  }

  public Integer getVersion() {
    return version;
  }

  public OTransactionId getTxId() {
    return txId;
  }

  public T getKey() {
    return key;
  }
}
