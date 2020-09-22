package com.orientechnologies.common.concur.lock;

import com.orientechnologies.orient.core.tx.OTransactionId;

import java.util.Objects;

public class OTxPromise<T> {
  private final T key;
  private final int version;
  private final OTransactionId txId;

  public OTxPromise(T key, int version, OTransactionId txId) {
    this.key = key;
    this.version = version;
    this.txId = txId;
  }

  public int getVersion() {
    return version;
  }

  public OTransactionId getTxId() {
    return txId;
  }

  public T getKey() {
    return key;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;
    OTxPromise<?> that = (OTxPromise<?>) o;
    return version == that.version && Objects.equals(key, that.key) && Objects.equals(txId, that.txId);
  }

  @Override
  public int hashCode() {
    return Objects.hash(key, version, txId);
  }
}
