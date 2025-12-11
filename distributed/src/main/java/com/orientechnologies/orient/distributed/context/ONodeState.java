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

  public ONodeState(
      ONodeId current,
      OGroupId groupId,
      int minimumQuorum,
      OStateStore store,
      ODatabaseStateChangeListener listener) {
    sequenceManager = new OTransactionSequenceManager(current, 3);
    state = new OAppliedState(3);
    log = new ODistributedMessageLogMemory();
    promised = new OPromisedDistributedOpsImpl();
    coordinated = new OCoordinatedDistributedOpsImpl(current, groupId, minimumQuorum);
    nodeId = current;
    this.store = store;
    this.databaseTopology = new ODatabasesTopologyState(listener);
  }

  public ODiscoverAction initFromStore() {
    Optional<ONodeStateStore> load = store.loadState();
    if (load.isPresent()) {
      coordinated.load(load.get());
    }
    Optional<byte[]> seq = store.loadSequence();
    if (seq.isPresent()) {
      sequenceManager.fill(OTxMetadataHolderImpl.read(seq.get()).getStatus());
    }
    return coordinated.discoverNode(nodeId);
  }

  public Optional<OOperationStart> start(OCompleteAction action) {
    Optional<OTransactionIdPromise> prom = this.sequenceManager.next();
    if (prom.isPresent()) {
      return Optional.of(this.coordinated.start(prom.get(), action));
    } else {
      return Optional.empty();
    }
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
    this.coordinated.completeExecution(promise);
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

  public Optional<OAcceptResult> promiseRegister(ONodeId node, long version) {
    return this.coordinated.promiseRegister(node, version);
  }

  public Set<ONodeId> enstablish(OGroupId groupId, Set<ONodeId> candidates) {
    return this.coordinated.enstablish(groupId, candidates);
  }

  public Optional<OAcceptResult> validateEnstablish(OGroupId groupId, Set<ONodeId> candidates) {
    return this.coordinated.validateEnstablish(groupId, candidates);
  }

  public Optional<OTransactionIdPromise> startEnstablish(
      Set<ONodeId> nodes, OCompleteAction action) {
    Optional<OTransactionIdPromise> prom = this.sequenceManager.next();
    if (prom.isPresent()) {
      this.coordinated.startEstablish(prom.get(), nodes, action);
    }
    return prom;
  }

  public ODiscoverAction nodeJoinStart(ONodeId node, ONodeStateNetwork state) {
    ODiscoverAction action = this.coordinated.nodeJoinStart(node, state.getTopology());
    if (action.applyDatabaseState()) {
      if (state.getTopology().isMerge()) {
        this.databaseTopology.mergeNetworkState(state.getDatabases());
      } else {
        this.databaseTopology.receiverNetworkState(state.getDatabases());
      }
    }
    return action;
  }

  public ONodeStateNetwork getNetworkState() {
    return new ONodeStateNetwork(
        this.coordinated.getNetworkState(), this.databaseTopology.getNetworkState());
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
      Set<ONodeId> partecipants,
      int minimumQuorum) {
    if (!partecipants.contains(nodeId)) {
      return Optional.of(new OMissingNode());
    }
    return this.databaseTopology.promiseDeclare(
        promise, databaseId, database, partecipants, minimumQuorum);
  }

  public void declareDatabase(
      OTransactionIdPromise promise,
      ODatabaseId dbId,
      String database,
      Set<ONodeId> partecipants,
      int minimumQuorum) {
    this.databaseTopology.declareDatabase(promise, dbId, database, partecipants, minimumQuorum);
  }

  public void cancelDatabase(OTransactionIdPromise promise, ODatabaseId dbId, String database) {
    this.databaseTopology.cancelPomise(promise, dbId, database);
  }

  public ODatabasesTopologyState getDatabaseTopology() {
    return databaseTopology;
  }

  public Set<ONodeId> getNetworkMemebers() {
    return coordinated.getMembers();
  }

  public Optional<OAcceptResult> promiseAddDatabaseMember(
      ODatabaseId dbId, ONodeId node, long version) {
    return this.databaseTopology.promiseAddMember(dbId, node, version);
  }

  public void addDatabaseMember(ODatabaseId dbId, ONodeId node, ONodeRole role, long version) {
    this.databaseTopology.addDatabaseMember(dbId, node, role, version);
  }

  public void cancelAddDatabaseMember(ODatabaseId dbId, ONodeId node) {
    // TODO Auto-generated method stub

  }
}
