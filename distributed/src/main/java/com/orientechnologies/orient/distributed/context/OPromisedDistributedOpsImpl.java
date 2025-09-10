package com.orientechnologies.orient.distributed.context;

import com.orientechnologies.orient.core.transaction.OTransactionIdPromise;
import com.orientechnologies.orient.distributed.context.coordination.message.ODistributedMessage;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

public class OPromisedDistributedOpsImpl implements OPromisedDistributedOps {

  private final Map<OTransactionIdPromise, ODistributedMessage> promised;
  private final Map<OTransactionIdPromise, ODistributedMessage> notPromised;

  public OPromisedDistributedOpsImpl() {
    this.promised = new ConcurrentHashMap<>();
    this.notPromised = new ConcurrentHashMap<>();
  }

  @Override
  public void addPromised(ODistributedMessage message) {
    this.promised.put(message.getPromiseId(), message);
  }

  @Override
  public ODistributedMessage getPromised(OTransactionIdPromise promise) {
    return this.promised.get(promise);
  }

  @Override
  public Optional<ODistributedMessage> removePromised(OTransactionIdPromise promise) {
    return Optional.of(this.promised.remove(promise));
  }

  @Override
  public void addNotPromised(ODistributedMessage message) {
    this.notPromised.put(message.getPromiseId(), message);
  }

  @Override
  public ODistributedMessage getNotPromised(OTransactionIdPromise promise) {
    return this.notPromised.get(promise);
  }

  @Override
  public Optional<ODistributedMessage> removeNotPromised(OTransactionIdPromise promise) {
    return Optional.of(this.notPromised.remove(promise));
  }
}
