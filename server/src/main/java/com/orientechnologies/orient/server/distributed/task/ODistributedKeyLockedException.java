package com.orientechnologies.orient.server.distributed.task;

import com.orientechnologies.common.concur.ONeedRetryException;

public class ODistributedKeyLockedException extends ONeedRetryException {

  private String key;
  private String node;

  public ODistributedKeyLockedException(final ODistributedRecordLockedException exception) {
    super(exception);
  }

  public ODistributedKeyLockedException(String localNodeName, Object key) {
    super("Cannot acquire lock on key '" + key + "' on server '" + localNodeName + "'.");
    this.key = key.toString();
    this.node = localNodeName;
  }

  public String getNode() {
    return node;
  }

  public String getKey() {
    return key;
  }
}
