package com.orientechnologies.orient.distributed.context.coordination;

import com.orientechnologies.orient.core.transaction.ODatabaseId;
import com.orientechnologies.orient.core.transaction.OGroupId;
import com.orientechnologies.orient.core.transaction.ONodeId;
import com.orientechnologies.orient.core.transaction.OTransactionId;
import com.orientechnologies.orient.core.transaction.OTransactionIdPromise;
import com.orientechnologies.orient.core.transaction.OTransactionSequenceManager;
import com.orientechnologies.orient.core.tx.OTransactionSequenceStatus;
import com.orientechnologies.orient.core.tx.ValidationResult;
import com.orientechnologies.orient.distributed.context.ONodeStateStore;
import com.orientechnologies.orient.distributed.context.ONodeStateUpdated;
import com.orientechnologies.orient.distributed.context.coordination.OResponseCollector.CompleteInfo;
import com.orientechnologies.orient.distributed.context.coordination.action.OCompleteAction;
import com.orientechnologies.orient.distributed.context.coordination.dbs.ODatabaseState;
import com.orientechnologies.orient.distributed.context.coordination.dbs.ODatabaseStateChangeListener;
import com.orientechnologies.orient.distributed.context.coordination.dbs.ODatabasesTopologyState;
import com.orientechnologies.orient.distributed.context.coordination.dbs.OStateAction;
import com.orientechnologies.orient.distributed.context.coordination.message.ODistributedMessage;
import com.orientechnologies.orient.distributed.context.coordination.message.operation.OAddNodeInfo;
import com.orientechnologies.orient.distributed.context.coordination.message.state.ONodeStateNetwork;
import com.orientechnologies.orient.distributed.context.coordination.result.OAcceptResult;
import com.orientechnologies.orient.distributed.context.coordination.result.OAlreadyPromised;
import com.orientechnologies.orient.distributed.context.coordination.result.OInvalidSequential;
import com.orientechnologies.orient.distributed.context.coordination.result.OMissingNode;
import com.orientechnologies.orient.distributed.context.coordination.sync.OSyncId;
import com.orientechnologies.orient.distributed.context.coordination.sync.OSyncInfo;
import com.orientechnologies.orient.distributed.context.coordination.sync.OSyncState;
import com.orientechnologies.orient.distributed.context.coordination.topology.ODiscoverAction;
import com.orientechnologies.orient.distributed.context.coordination.topology.OTopologyManager;
import com.orientechnologies.orient.distributed.db.OSyncMode;
import com.orientechnologies.orient.server.distributed.ODistributedException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public class OCoordinatedDistributedOpsImpl implements OCoordinatedDistributedOps {

  private final OTopologyManager topology;
  private final OTransactionSequenceManager sequenceManager;
  private final OPromisedDistributedOps promised;
  private final Map<OTransactionIdPromise, OResponseCollector> coordination;
  private final ODatabasesTopologyState databaseTopology;
  private ONodeStateUpdated updateLister;

  public OCoordinatedDistributedOpsImpl(
      ONodeId current,
      OGroupId groupId,
      int minimumQuorum,
      ODatabaseStateChangeListener listener,
      ONodeStateUpdated updateLister) {
    this.topology = new OTopologyManager(current, groupId, minimumQuorum);
    this.sequenceManager = new OTransactionSequenceManager(current, 3);
    this.promised = new OPromisedDistributedOpsImpl();
    this.coordination = new HashMap<>();
    this.databaseTopology = new ODatabasesTopologyState(listener);
    this.updateLister = updateLister;
  }

  @Override
  public ODiscoverAction discoverNode(ONodeId node) {
    return this.topology.nodeDiscovered(node);
  }

  public synchronized void registerNode(ONodeId node, long version) {
    this.topology.register(node, version);
    notifyUpdate();
  }

  public void unregisterNode(ONodeId node, long version) {
    Optional<CompleteInfo> action = Optional.empty();
    synchronized (this) {
      this.topology.unregister(node, version);
      Iterator<OResponseCollector> iterator = coordination.values().iterator();
      while (iterator.hasNext()) {
        OResponseCollector coll = iterator.next();
        action = coll.disconnected(node);
        if (coll.isTotallyFinished()) {
          iterator.remove();
        }
      }
    }
    notifyUpdate();
    if (action.isPresent()) {
      CompleteInfo info = action.get();
      info.action().failure(info.promise(), info.nodes(), info.result());
    }
  }

  @Override
  public synchronized Optional<OOperationStart> start(OCompleteAction action) {
    if (this.topology.enoughNodes()) {
      throw new ODistributedException(
          String.format(
              "No enough nodes to coordinate an operation with quorum: %d know nodes:%s",
              this.topology.getMinimumQuorum(), this.getNetworkMembers().toString()));
    }

    Optional<OTransactionIdPromise> prom = this.sequenceManager.next();
    if (prom.isPresent()) {
      var promise = prom.get();
      Set<ONodeId> nodes = Collections.unmodifiableSet(new HashSet<>(topology.getMembers()));
      coordination.put(
          promise, new OResponseCollector(action, promise, topology.getQuorum(), nodes));
      return Optional.of(new OOperationStart(promise, nodes));
    } else {
      return Optional.empty();
    }
  }

  public synchronized Optional<OAcceptResult> receive(ODistributedMessage message) {
    ValidationResult result = sequenceManager.validate(message.getPromiseId());
    return switch (result) {
      case VALID -> {
        this.promised.addPromised(message);
        yield Optional.empty();
      }
      case ALREADY_PRESENT -> {
        // Already present ... maybe do nothing, already done
        long current =
            sequenceManager.debugGetSequence(message.getPromiseId().getId().getPosition());
        yield Optional.of(
            new OInvalidSequential(current, message.getPromiseId().getId().getSequence()));
      }
      case ALREADY_PROMISED -> {
        // Fail for promised to someone else this track it anyway in case of minority in quorum
        this.promised.addNotPromised(message);
        yield Optional.of(new OAlreadyPromised());
      }
      case MISSING_PREVIOUS -> {
        // wait for previous one, track it anyway
        this.promised.addNotPromised(message);
        long current =
            sequenceManager.debugGetSequence(message.getPromiseId().getId().getPosition());
        yield Optional.of(
            new OInvalidSequential(current, message.getPromiseId().getId().getSequence()));
      }
    };
  }

  public synchronized Optional<ODistributedMessage> consensusFailure(
      OTransactionIdPromise promise) {
    boolean promised = sequenceManager.notifyFailure(promise);
    if (promised) {
      return this.promised.removePromised(promise);
    }
    return Optional.empty();
  }

  public synchronized Optional<ODistributedMessage> consensusSuccess(
      OTransactionIdPromise promise) {
    // TODO: if received the confirmation for not promised message have to cancel eventual
    // promised message and try to promised and apply currently confirmed message.
    ValidationResult result = sequenceManager.notifySuccess(promise);
    return switch (result) {
      case VALID -> {
        ODistributedMessage message = this.promised.getPromised(promise);
        yield Optional.ofNullable(message);
      }
      case ALREADY_PRESENT -> {
        // Already present ... maybe do nothing, already done
        finalize(promise);
        yield Optional.empty();
      }
      case ALREADY_PROMISED -> {
        // Fail for promised to someone else
        yield Optional.empty();
      }
      case MISSING_PREVIOUS -> {
        // wait for previous one
        yield Optional.empty();
      }
    };
  }

  private void finalize(OTransactionIdPromise promise) {
    this.promised.removePromised(promise);
  }

  @Override
  public void nodeSuccess(ONodeId node, OTransactionIdPromise promise) {
    Optional<CompleteInfo> action = Optional.empty();
    synchronized (this) {
      OResponseCollector coll = coordination.get(promise);
      if (coll != null) {
        action = coll.receive(node);
        if (coll.isTotallyFinished()) {
          coordination.remove(promise);
        }
      }
    }
    if (action.isPresent()) {
      CompleteInfo info = action.get();
      info.action().success(info.promise(), info.nodes());
    }
  }

  @Override
  public void nodeFailure(ONodeId node, OTransactionIdPromise promise, OAcceptResult acceptResult) {
    Optional<CompleteInfo> action = Optional.empty();
    synchronized (this) {
      OResponseCollector coll = coordination.get(promise);
      if (coll != null) {
        action = coll.fail(node, acceptResult);
        if (coll.isTotallyFinished()) {
          coordination.remove(promise);
        }
      }
    }
    if (action.isPresent()) {
      CompleteInfo info = action.get();
      info.action().failure(promise, this.topology.getMembers(), info.result());
    }
  }

  public void completeExecution(OTransactionIdPromise promise) {
    finalize(promise);
    Optional<CompleteInfo> action = Optional.empty();
    synchronized (this) {
      OResponseCollector coll = coordination.get(promise);
      if (coll != null) {
        action = coll.applied();
        if (coll.isTotallyFinished()) {
          coordination.remove(promise);
        }
      }
    }
    if (action.isPresent()) {
      CompleteInfo info = action.get();
      info.action().complete(info.promise(), info.nodes(), info.result());
    }
  }

  @Override
  public Set<ONodeId> getNetworkMembers() {
    return topology.getMembers();
  }

  @Override
  public Optional<OAcceptResult> validateRegisterNode(ONodeId node, long version) {
    return topology.promiseRegister(node, version);
  }

  @Override
  public Set<ONodeId> enstablish(OGroupId groupId, Set<ONodeId> candidates) {
    Set<ONodeId> result = this.topology.finalizeEnstablish(groupId, candidates);
    notifyUpdate();
    return result;
  }

  @Override
  public Optional<OAcceptResult> validateEnstablish(OGroupId groupId, Set<ONodeId> candidates) {
    return this.topology.validateEnstablish(groupId, candidates);
  }

  @Override
  public synchronized Optional<OTransactionIdPromise> startEstablish(
      Set<ONodeId> nodes, OCompleteAction action) {
    Optional<OTransactionIdPromise> prom = this.sequenceManager.next();
    if (prom.isPresent()) {
      coordination.put(
          prom.get(), new OResponseCollector(action, prom.get(), topology.getQuorum(), nodes));
    }
    return prom;
  }

  @Override
  public synchronized ODiscoverAction nodeJoinStart(ONodeId node, ONodeStateNetwork state) {
    var action = this.topology.nodeJoinStart(node, state.topology());
    boolean notifyUpdate = false;
    if (action.applyDatabaseState()) {
      if (state.topology().isMerge()) {
        this.databaseTopology.mergeNetworkState(state.databases());
      } else {
        this.databaseTopology.receiverNetworkState(state.databases());
      }
      notifyUpdate = true;
    }
    if (action.applySequenceState()) {
      this.sequenceManager.fill(state.sequenceStatus());
      notifyUpdate = true;
    }
    if (notifyUpdate) {
      notifyUpdate();
    }
    return action;
  }

  @Override
  public ONodeStateNetwork getNetworkState() {
    return new ONodeStateNetwork(
        this.topology.getNetworkState(),
        this.databaseTopology.getNetworkState(),
        this.sequenceManager.currentStatus());
  }

  public void load(ONodeStateStore state) {
    if (state.network().isPresent()) {
      this.topology.load(state.network().get());
    }
    if (state.databases().isPresent()) {
      databaseTopology.load(state.databases().get());
    }
    if (state.sequence().isPresent()) {
      this.sequenceManager.fill(state.sequence().get());
    }
  }

  @Override
  public void cancelRegisterNode() {
    this.topology.cancelRegisterPromise();
  }

  @Override
  public void cancelEnstablish() {
    this.topology.cancelEnstablish();
  }

  public Optional<OAcceptResult> validateDeclareDatabase(
      OTransactionIdPromise promise,
      ODatabaseId databaseId,
      String database,
      Set<ONodeId> partecipants,
      int minimumQuorum) {
    if (!partecipants.contains(topology.getNodeId())) {
      return Optional.of(new OMissingNode());
    }
    return this.databaseTopology.validateDeclare(
        promise, databaseId, database, partecipants, minimumQuorum);
  }

  public void declareDatabase(
      OTransactionIdPromise promise,
      ODatabaseId dbId,
      String database,
      Set<ONodeId> partecipants,
      int minimumQuorum) {
    this.databaseTopology.declareDatabase(promise, dbId, database, partecipants, minimumQuorum);
    notifyUpdate();
  }

  public void cancelDeclareDatabase(
      OTransactionIdPromise promise, ODatabaseId dbId, String database) {
    this.databaseTopology.cancelPomise(promise, dbId, database);
  }

  public ODatabasesTopologyState getDatabaseTopology() {
    return databaseTopology;
  }

  public Optional<OAcceptResult> validateAddDatabaseMember(
      ODatabaseId dbId, List<OAddNodeInfo> nodes, long version) {
    return this.databaseTopology.validateAddMember(dbId, nodes, version);
  }

  public void addDatabaseMember(ODatabaseId dbId, List<OAddNodeInfo> nodes, long version) {
    this.databaseTopology.addDatabaseMember(dbId, nodes, version);
    notifyUpdate();
  }

  public void cancelAddDatabaseMember(ODatabaseId dbId, List<OAddNodeInfo> nodes) {
    this.databaseTopology.cancelAddDatabaseMember(dbId, nodes);
  }

  @Override
  public Optional<OAcceptResult> validateSetState(
      ODatabaseId dbId, ONodeId nodeId, ODatabaseState state, long version) {
    return this.databaseTopology.validateSetState(dbId, nodeId, state, version);
  }

  @Override
  public void setState(ODatabaseId db, ONodeId node, ODatabaseState state, long version) {
    this.databaseTopology.setState(db, node, state, version);
  }

  @Override
  public void cancelSetState(ODatabaseId dbId, ONodeId nodeId, long version) {
    this.databaseTopology.cancelSetState(dbId, nodeId, version);
  }

  @Override
  public Optional<OSyncInfo> newSync(ODatabaseId dbId) {
    return this.databaseTopology.newSync(dbId);
  }

  @Override
  public boolean acceptSync(ONodeId sender, ONodeId receiver, ODatabaseId dbId, OSyncId syncId) {
    return this.databaseTopology.acceptSync(sender, receiver, dbId, syncId);
  }

  @Override
  public Optional<OSyncState> canSync(
      ONodeId sender,
      ONodeId receiver,
      ODatabaseId dbId,
      OSyncId syncId,
      boolean canSync,
      OSyncMode mode,
      Optional<OTransactionSequenceStatus> sequenceStatus) {
    return this.databaseTopology.canSync(
        sender, receiver, dbId, syncId, canSync, mode, sequenceStatus);
  }

  @Override
  public OSyncState startSend(
      ONodeId to,
      ONodeId from,
      ODatabaseId dbId,
      OSyncId syncId,
      OSyncMode mode,
      Optional<OTransactionSequenceStatus> sequenceStatus) {
    return this.databaseTopology.startSend(to, from, dbId, syncId, mode, sequenceStatus);
  }

  @Override
  public OSyncState getSyncState(OSyncId syncId) {
    return this.databaseTopology.getSyncState(syncId);
  }

  public boolean executeOnOneOnline(ODatabaseId dbId, OStateAction execute) {
    return this.databaseTopology.executeOnOneOnline(dbId, execute);
  }

  @Override
  public boolean waitOnlineQuorum(ODatabaseId dbId, Optional<Long> timeout)
      throws InterruptedException {
    return this.databaseTopology.waitOnlineQuorum(dbId, timeout);
  }

  @Override
  public boolean isApplied(OTransactionId txId) {
    return this.sequenceManager.isApplied(txId);
  }

  @Override
  public void completeSync(OSyncId syncId) {
    this.databaseTopology.completeSync(syncId);
  }

  public synchronized ONodeStateStore getStore() {
    return new ONodeStateStore(
        Optional.of(sequenceManager.currentStatus()),
        Optional.of(topology.getStore()),
        Optional.of(databaseTopology.getStore()));
  }

  private void notifyUpdate() {
    this.updateLister.update(getStore());
  }
}
