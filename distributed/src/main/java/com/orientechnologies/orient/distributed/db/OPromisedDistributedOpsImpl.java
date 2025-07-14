package com.orientechnologies.orient.distributed.db;

import com.orientechnologies.orient.core.tx.OTransactionIdPromise;
import com.orientechnologies.orient.server.distributed.ODistributedMessage;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class OPromisedDistributedOpsImpl implements OPromisedDistributedOps {

  private Map<OTransactionIdPromise, ODistributedMessage> promised;

  public OPromisedDistributedOpsImpl() {
    this.promised = new ConcurrentHashMap<>();
  }

  @Override
  public void add(ODistributedMessage message) {
    this.promised.put(message.getPromiseId(), message);
  }

  @Override
  public ODistributedMessage get(OTransactionIdPromise promise) {
    return this.promised.get(promise);
  }

  @Override
  public void remove(OTransactionIdPromise promise) {
    this.promised.remove(promise);
  }
}
