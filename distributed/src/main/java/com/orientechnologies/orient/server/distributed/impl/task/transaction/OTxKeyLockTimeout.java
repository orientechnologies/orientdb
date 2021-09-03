package com.orientechnologies.orient.server.distributed.impl.task.transaction;

public class OTxKeyLockTimeout implements OTransactionResultPayload {
  public static final int ID = 7;
  private String node;
  private String key;

  public OTxKeyLockTimeout(String node, String key) {
    this.node = node;
    this.key = key;
  }

  @Override
  public int getResponseType() {
    return OTxKeyLockTimeout.ID;
  }

  public String getKey() {
    return key;
  }

  public String getNode() {
    return node;
  }
}
