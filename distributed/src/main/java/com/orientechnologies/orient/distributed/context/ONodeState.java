package com.orientechnologies.orient.distributed.context;

import com.orientechnologies.orient.core.transaction.OGroupId;
import com.orientechnologies.orient.core.transaction.ONodeId;
import com.orientechnologies.orient.core.transaction.OTransactionId;
import com.orientechnologies.orient.core.transaction.OTransactionIdPromise;
import com.orientechnologies.orient.core.transaction.OTransactionSequenceManager;
import com.orientechnologies.orient.core.tx.OTxMetadataHolderImpl;
import com.orientechnologies.orient.core.tx.ValidationResult;
import com.orientechnologies.orient.distributed.context.coordination.message.ODistributedMessage;
import com.orientechnologies.orient.distributed.context.coordination.message.ONodeStateNetwork;
import com.orientechnologies.orient.distributed.context.coordination.result.OAcceptResult;
import com.orientechnologies.orient.distributed.context.topology.ODiscoverAction;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

public class ONodeState {

  private final OTransactionSequenceManager sequenceManager;
  private final ODistributedMessageLog log;
  private final OPromisedDistributedOps promised;
  private final OCoordinatedDistributedOps coordinated;
  private final ONodeId nodeId;
  private final OAppliedState state;
  private final OStateStore store;

  public ONodeState(ONodeId current, int minimumQuorum, OStateStore store) {
    sequenceManager = new OTransactionSequenceManager(current, 3);
    state = new OAppliedState(3);
    log = new ODistributedMessageLogMemory();
    promised = new OPromisedDistributedOpsImpl();
    coordinated = new OCoordinatedDistributedOpsImpl(minimumQuorum);
    nodeId = current;
    this.store = store;
    initFromStore();
  }

  public void initFromStore() {
    Optional<ONodeStateStore> load = store.loadState();
    if (load.isPresent()) {
      coordinated.load(load.get());
    }
    coordinated.discoverNode(nodeId);
    Optional<byte[]> seq = store.loadSequence();
    if (seq.isPresent()) {
      sequenceManager.fill(OTxMetadataHolderImpl.read(seq.get()).getStatus());
    }
  }

  public OOperationStart start(OCompleteAction action) {
    Optional<OTransactionIdPromise> prom = this.sequenceManager.next();
    return this.coordinated.start(prom.get(), action);
  }

  public void success(ONodeId node, OTransactionIdPromise promise) {
    this.coordinated.success(node, promise);
  }

  public void failure(ONodeId node, OTransactionIdPromise promise, OAcceptResult acceptResult) {
    this.coordinated.failure(node, promise, acceptResult);
  }

  public ODiscoverAction discover(ONodeId node) {
    return this.coordinated.discoverNode(node);
  }

  public void register(ONodeId node, long version) {
    this.coordinated.registerNode(node, version);
  }

  public void unregister(ONodeId node, long version) {
    this.coordinated.unregisterNode(node, version);
  }

  private void fill(Optional<byte[]> lastMetadata) {
    lastMetadata.ifPresent(
        (data) -> sequenceManager.fill(OTxMetadataHolderImpl.read(data).getStatus()));
  }

  public boolean receive(ODistributedMessage message) {
    ValidationResult result = sequenceManager.validate(message.getPromiseId());
    switch (result) {
      case VALID -> {
        this.log.log(message);
        this.promised.add(message);
        return true;
      }
      case ALREADY_PRESENT -> {
        // Already present ... maybe do nothing, already done
        return false;
      }
      case ALREADY_PROMISED -> {
        // Fail for promised to someone else
        return false;
      }
      case MISSING_PREVIOUS -> {
        // wait for previous one
        return false;
      }
    }
    return false;
  }

  public void receiveFailure(OTransactionIdPromise promise) {
    boolean promised = sequenceManager.notifyFailure(promise);
    if (promised) {
      finalize(promise);
    }
  }

  public ODistributedMessage receiveSuccess(OTransactionIdPromise promise) {
    // TODO: the verification of success also close the promise, maybe is better to close
    // the promise after the execution;
    ValidationResult result = sequenceManager.notifySuccess(promise);
    switch (result) {
      case VALID -> {
        ODistributedMessage message = this.promised.get(promise);
        return message;
      }
      case ALREADY_PRESENT -> {
        // Already present ... maybe do nothing, already done
        finalize(promise);
      }
      case ALREADY_PROMISED -> {
        // Fail for promised to someone else
      }
      case MISSING_PREVIOUS -> {
        // wait for previous one
      }
    }

    return null;
  }

  private void finalize(OTransactionIdPromise promise) {
    this.promised.remove(promise);
  }

  public void complete(OTransactionIdPromise promise) {
    finalize(promise);
    this.coordinated.completeExecution(promise.getId());
    this.state.complete(promise.getId());
  }

  public List<ODistributedMessage> recover(List<OTransactionId> ids) {
    return this.log.recover(ids);
  }

  public ONodeId getNodeId() {
    return nodeId;
  }

  public void waitComplete(OTransactionIdPromise promise) {
    Optional<CountDownLatch> value = this.state.watch(promise.getId());
    if (value.isPresent()) {
      try {
        // TODO: use timeout
        value.get().await();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
  }

  public boolean promiseRegister(ONodeId node, long version) {
    return this.coordinated.promiseRegister(node, version);
  }

  public void enstablish(OGroupId groupId, Set<ONodeId> candidates) {
    this.coordinated.enstablish(groupId, candidates);
  }

  public Optional<OAcceptResult> validateEnstablish(OGroupId groupId, Set<ONodeId> candidates) {
    return this.coordinated.validateEnstablish(groupId, candidates);
  }

  public OTransactionIdPromise startEnstablish(Set<ONodeId> nodes, OCompleteAction action) {
    Optional<OTransactionIdPromise> prom = this.sequenceManager.next();
    this.coordinated.startEstablish(prom.get(), nodes, action);
    return prom.get();
  }

  public ODiscoverAction checkExternNodeState(ONodeId node, ONodeStateNetwork state) {
    return this.coordinated.checkExternNodeState(node, state);
  }

  public ONodeStateNetwork getNetworkState() {
    return this.coordinated.getNetworkState();
  }
}
