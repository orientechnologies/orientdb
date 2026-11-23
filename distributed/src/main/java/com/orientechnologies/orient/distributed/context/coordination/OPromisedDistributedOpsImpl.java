package com.orientechnologies.orient.distributed.context.coordination;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.log.OLogger;
import com.orientechnologies.orient.core.id.ONodeId;
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
  private final Map<ONodeId, Map<OTransactionId, ODistributedMessage>> promisedByNode;
  private final Map<ONodeId, Map<OTransactionId, ODistributedMessage>> notPromisedByNode;
  private final Map<OTransactionId, ODistributedMessage> promisedById;

  public OPromisedDistributedOpsImpl() {
    this.promised = new ConcurrentHashMap<>();
    this.promisedByNode = new ConcurrentHashMap<>();
    this.notPromised = new ConcurrentHashMap<>();
    this.notPromisedByNode = new ConcurrentHashMap<>();
    this.promisedById = new ConcurrentHashMap<>();
  }

  @Override
  public void addPromised(ODistributedMessage message) {
    this.promised.put(message.getPromiseId(), message);
    this.promisedById.put(message.getPromiseId().getId(), message);
    var perNode =
        this.promisedByNode.computeIfAbsent(
            message.getPromiseId().getCoordinator(), node -> new ConcurrentHashMap<>());
    perNode.put(message.getPromiseId().getId(), message);
  }

  @Override
  public ODistributedMessage getPromised(OTransactionIdPromise promise) {
    return this.promised.get(promise);
  }

  @Override
  public void finalizeSuccess(OTransactionIdPromise promise) {
    // It may be both promised and not promised handle both cases.
    removePromised(promise);
    removeNotPromised(promise);
  }

  @Override
  public Optional<ODistributedMessage> cancel(OTransactionIdPromise promise) {
    // It may be both promised and not promised handle both cases.
    var removed = removePromised(promise);
    if (removed.isPresent()) {
      return removed;
    }
    return removeNotPromised(promise);
  }

  public Optional<ODistributedMessage> removePromised(OTransactionIdPromise promise) {
    var messages = this.promisedByNode.get(promise.getCoordinator());
    if (messages != null) {
      messages.remove(promise.getId());
    }
    this.promisedById.remove(promise.getId());
    return Optional.ofNullable(this.promised.remove(promise));
  }

  @Override
  public void addNotPromised(ODistributedMessage message) {
    this.notPromised.put(message.getPromiseId(), message);
    var perNode =
        this.notPromisedByNode.computeIfAbsent(
            message.getPromiseId().getCoordinator(), node -> new ConcurrentHashMap<>());
    perNode.put(message.getPromiseId().getId(), message);
  }

  @Override
  public ODistributedMessage getNotPromised(OTransactionIdPromise promise) {
    return this.notPromised.get(promise);
  }

  public Optional<ODistributedMessage> removeNotPromised(OTransactionIdPromise promise) {
    var messages = this.notPromisedByNode.get(promise.getCoordinator());
    if (messages != null) {
      messages.remove(promise.getId());
    }
    return Optional.ofNullable(this.notPromised.remove(promise));
  }

  @Override
  public Optional<Map<OTransactionId, ODistributedMessage>> getPromised(ONodeId node) {
    return Optional.ofNullable(this.promisedByNode.get(node));
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
    var notPromised = notPromisedByNode.remove(node);
    if (notPromised != null) {
      for (var not : notPromised.values()) {
        this.notPromised.remove(not.getPromiseId());
      }
    }

    var promised = promisedByNode.remove(node);
    if (promised != null) {
      for (var prom : promised.values()) {
        this.promised.remove(prom.getPromiseId());
        this.promisedById.remove(prom.getPromiseId().getId());
      }
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
