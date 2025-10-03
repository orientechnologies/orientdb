package com.orientechnologies.orient.distributed.context;

import com.orientechnologies.orient.core.transaction.ODatabaseId;
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
import com.orientechnologies.orient.distributed.context.coordination.result.OMissingNode;
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
  private final ODatabasesTopologyState databaseTopology;

  public ONodeState(ONodeId current, int minimumQuorum, OStateStore store) {
    sequenceManager = new OTransactionSequenceManager(current, 3);
    state = new OAppliedState(3);
    log = new ODistributedMessageLogMemory();
    promised = new OPromisedDistributedOpsImpl();
    coordinated = new OCoordinatedDistributedOpsImpl(current, minimumQuorum);
    nodeId = current;
    this.store = store;
    this.databaseTopology = new ODatabasesTopologyState();
    initFromStore();
  }

  public void initFromStore() {
    Optional<ONodeStateStore> load = store.loadState();
    if (load.isPresent()) {
      coordinated.load(load.get());
    }
    // TODO: see action here ...
    ODiscoverAction action = coordinated.discoverNode(nodeId);
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

  public void register(ONodeId node, long version) {
    this.coordinated.registerNode(node, version);
  }

  public void unregister(ONodeId node, long version) {
    this.coordinated.unregisterNode(node, version);
  }

  public boolean receive(ODistributedMessage message) {
    ValidationResult result = sequenceManager.validate(message.getPromiseId());
    switch (result) {
      case VALID -> {
        this.log.log(message);
        this.promised.addPromised(message);
        return true;
      }
      case ALREADY_PRESENT -> {
        // Already present ... maybe do nothing, already done
        return false;
      }
      case ALREADY_PROMISED -> {
        // Fail for promised to someone else this track it anyway in case of minority in quorum
        this.promised.addNotPromised(message);
        return false;
      }
      case MISSING_PREVIOUS -> {
        // wait for previous one, track it anyway
        this.promised.addNotPromised(message);
        return false;
      }
    }
    return false;
  }

  public Optional<ODistributedMessage> receiveFailure(OTransactionIdPromise promise) {
    boolean promised = sequenceManager.notifyFailure(promise);
    if (promised) {
      return this.promised.removePromised(promise);
    }
    return Optional.empty();
  }

  public ODistributedMessage receiveSuccess(OTransactionIdPromise promise) {
    // TODO: if received the confirmation for not promised message have to cancel eventual
    // promised message and try to promised and apply currently confirmed message.
    ValidationResult result = sequenceManager.notifySuccess(promise);
    switch (result) {
      case VALID -> {
        ODistributedMessage message = this.promised.getPromised(promise);
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
    this.promised.removePromised(promise);
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

  public ODiscoverAction nodeJoinStart(ONodeId node, ONodeStateNetwork state) {
    return this.coordinated.nodeJoinStart(node, state);
  }

  public ONodeStateNetwork getNetworkState() {
    return this.coordinated.getNetworkState();
  }

  public void cancelRegisterPromise() {
    this.coordinated.cancelRegisterPromise();
  }

  public void cancelEnstablish() {
    this.coordinated.cancelEnstablish();
  }

  public Optional<OAcceptResult> promiseDeclare(
      OTransactionIdPromise promise,
      ODatabaseId databaseId,
      String database,
      Set<ONodeId> partecipants) {
    if (!partecipants.contains(nodeId)) {
      return Optional.of(new OMissingNode());
    }
    return this.databaseTopology.promiseDeclare(promise, databaseId, database);
  }

  public void declareDatabase(OTransactionIdPromise promise, ODatabaseId dbId, String database) {
    this.databaseTopology.declareDatabase(promise, dbId, database);
  }

  public void cancelDatabase(OTransactionIdPromise promise, ODatabaseId dbId, String database) {
    this.databaseTopology.cancelPomise(promise, dbId, database);
  }

  public ODatabasesTopologyState getDatabaseTopology() {
    return databaseTopology;
  }
}
