package com.orientechnologies.orient.distributed.context.coordination;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.log.OLogger;
import com.orientechnologies.orient.core.transaction.ONodeId;
import com.orientechnologies.orient.core.transaction.OTransactionId;
import com.orientechnologies.orient.core.transaction.OTransactionIdPromise;
import com.orientechnologies.orient.distributed.context.coordination.message.ODistributedMessage;
import java.util.ArrayList;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

public class OPromisedDistributedOpsImpl implements OPromisedDistributedOps {
  private static final OLogger logger =
      OLogManager.instance().logger(OPromisedDistributedOpsImpl.class);

  private final Map<OTransactionIdPromise, ODistributedMessage> promised;
  private final Map<OTransactionIdPromise, ODistributedMessage> notPromised;
  private final Map<ONodeId, Map<OTransactionId, ODistributedMessage>> primisedByNode;
  private final Map<OTransactionId, ODistributedMessage> promisedById;

  public OPromisedDistributedOpsImpl() {
    this.promised = new ConcurrentHashMap<>();
    this.primisedByNode = new ConcurrentHashMap<>();
    this.notPromised = new ConcurrentHashMap<>();
    this.promisedById = new ConcurrentHashMap<>();
  }

  @Override
  public void addPromised(ODistributedMessage message) {
    this.promised.put(message.getPromiseId(), message);
    this.promisedById.put(message.getPromiseId().getId(), message);
    var perNode =
        this.primisedByNode.computeIfAbsent(
            message.getPromiseId().getCoordinator(), node -> new ConcurrentHashMap<>());
    perNode.put(message.getPromiseId().getId(), message);
  }

  @Override
  public ODistributedMessage getPromised(OTransactionIdPromise promise) {
    return this.promised.get(promise);
  }

  @Override
  public void finalize(OTransactionIdPromise promise) {
    // It may be both promised and not promised handle both cases.
    removePromised(promise);
    removeNotPromised(promise);
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

  @Override
  public void dumpActive() {
    String active = "";
    for (var entry : this.promised.entrySet()) {
      active += entry.getValue() + "\n";
    }
    logger.debug("promised on missing sequence state: \n %s ", active);
  }

  @Override
  public ODisconnectAction nodeDisconnected(ONodeId node) {
    var promised = primisedByNode.get(node);
    if (promised != null) {
      return new ODisconnectAction.OReconsentPromised(new ArrayList<>(promised.values()));
    } else {
      return new ODisconnectAction.ONothingToDo();
    }
  }

  @Override
  public boolean isPromised(OTransactionId id) {
    return promisedById.containsKey(id);
  }
}
