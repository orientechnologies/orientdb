package com.orientechnologies.orient.core.storage.impl.local;

public final class OStartupMetadata {
  final long lastTxId;
  final byte[] txMetadata;

  public OStartupMetadata(long lastTxId, byte[] txMetadata) {
    this.lastTxId = lastTxId;
    this.txMetadata = txMetadata;
  }
}
