package com.orientechnologies.orient.distributed.context.coordination;

import com.orientechnologies.orient.core.transaction.ONodeId;
import com.orientechnologies.orient.core.transaction.OTransactionId;
import com.orientechnologies.orient.core.transaction.OTransactionIdPromise;
import com.orientechnologies.orient.distributed.context.coordination.message.ODistributedMessage;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

public class OPromisedDistributedOpsImpl implements OPromisedDistributedOps {

  private final Map<OTransactionIdPromise, ODistributedMessage> promised;
  private final Map<OTransactionIdPromise, ODistributedMessage> notPromised;
  private final Map<ONodeId, Map<OTransactionId, ODistributedMessage>> primisedByNode;

  public OPromisedDistributedOpsImpl() {
    this.promised = new ConcurrentHashMap<>();
    this.primisedByNode = new ConcurrentHashMap<>();
    this.notPromised = new ConcurrentHashMap<>();
  }

  @Override
  public void addPromised(ODistributedMessage message) {
    this.promised.put(message.getPromiseId(), message);
    var perNode =
        this.primisedByNode.computeIfAbsent(
            message.getPromiseId().getCoordinator(),
            (node) -> {
              return new ConcurrentHashMap<>();
            });
    perNode.put(message.getPromiseId().getId(), message);
  }

  @Override
  public ODistributedMessage getPromised(OTransactionIdPromise promise) {
    return this.promised.get(promise);
  }

  @Override
  public Optional<ODistributedMessage> removePromised(OTransactionIdPromise promise) {
    var messages = this.primisedByNode.get(promise.getCoordinator());
    if (messages != null) {
      messages.remove(promise.getId());
    }

    return Optional.ofNullable(this.promised.remove(promise));
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
    return Optional.ofNullable(this.notPromised.remove(promise));
  }

  @Override
  public Optional<Map<OTransactionId, ODistributedMessage>> getPromised(ONodeId node) {
    return Optional.ofNullable(this.primisedByNode.get(node));
  }
}
