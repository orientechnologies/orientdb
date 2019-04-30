package com.orientechnologies.orient.server.distributed.task;

import com.orientechnologies.common.concur.ONeedRetryException;
import com.orientechnologies.orient.server.distributed.ODistributedRequestId;

public class ODistributedKeyLockedException extends ONeedRetryException {

  private String                key;
  private String                node;

  public ODistributedKeyLockedException(final ODistributedRecordLockedException exception) {
    super(exception);
  }

  public ODistributedKeyLockedException(String localNodeName, Object key, long timeout) {
    super("Timeout (" + timeout + "ms) on acquiring lock on key '" + key + "' on server '" + localNodeName
        + "'.");
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


