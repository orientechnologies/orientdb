package com.orientechnologies.orient.distributed.context;

import com.orientechnologies.orient.core.transaction.OTransactionIdPromise;
import com.orientechnologies.orient.distributed.context.coordination.message.ODistributedMessage;
import java.util.Map;
import java.util.Optional;
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
  public Optional<ODistributedMessage> remove(OTransactionIdPromise promise) {
    return Optional.of(this.promised.remove(promise));
  }
}
