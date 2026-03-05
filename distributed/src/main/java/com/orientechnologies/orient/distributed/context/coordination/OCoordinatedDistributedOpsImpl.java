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
import com.orientechnologies.orient.server.distributed.OLoggerDistributed;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public class OCoordinatedDistributedOpsImpl implements OCoordinatedDistributedOps {
  private static final OLoggerDistributed logger =
      OLoggerDistributed.logger(OCoordinatedDistributedOpsImpl.class);
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
    this.databaseTopology = new ODatabasesTopologyState(listener, current);
    this.updateLister = updateLister;
  }

  @Override
  public ODiscoverAction discoverNode(ONodeId node) {
    return this.topology.nodeDiscovered(node);
  }

  public synchronized void registerNode(ONodeId node, long version, OTransactionIdPromise promise) {
    this.topology.register(node, version, promise);
    notifyUpdate();
  }

  @Override
  public Optional<OAcceptResult> validateUnregisterNode(
      ONodeId node, OVersion version, OTransactionIdPromise promise) {
    return topology.promiseUnregister(node, version, promise);
  }

  public ODisconnectAction unregisterNode(
      ONodeId node, OVersion version, OTransactionIdPromise promise) {
    var action = nodeDisconnected(node);

    synchronized (this) {
      this.topology.unregister(node, version, promise);
    }
    notifyUpdate();
    return action;
  }

  @Override
  public void cancelUnregisterNode(OTransactionIdPromise promise) {
    this.topology.cancelUnregisterPromise(promise);
  }

  private synchronized List<CompleteInfo> disconnectCoordinated(ONodeId node) {
    List<CompleteInfo> actions = new ArrayList<>();
    Iterator<OResponseCollector> iterator = coordination.values().iterator();
    while (iterator.hasNext()) {
      OResponseCollector coll = iterator.next();
      var action = coll.disconnected(node);
      if (action.isPresent()) {
        actions.add(action.get());
      }
      if (coll.isTotallyFinished()) {
        iterator.remove();
      }
    }
    return actions;
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
      coordination.put(promise, action.newResponseCollector(promise, topology.getQuorum(), nodes));
      return Optional.of(new OOperationStart(promise, nodes));
    } else {
      dumpActive();
      return Optional.empty();
    }
  }

  @Override
  public Optional<OOperationStart> restart(OTransactionIdPromise current, OCompleteAction action) {
    if (this.topology.enoughNodes()) {
      throw new ODistributedException(
          String.format(
              "No enough nodes to coordinate an operation with quorum: %d know nodes:%s",
              this.topology.getMinimumQuorum(), this.getNetworkMembers().toString()));
    }

    var promise = current.retrySequence(topology.getNodeId());
    // It should use previous quorum I think
    Set<ONodeId> nodes = Collections.unmodifiableSet(new HashSet<>(topology.getMembers()));
    coordination.put(promise, action.newResponseCollector(promise, topology.getQuorum(), nodes));
    return Optional.of(new OOperationStart(promise, nodes));
  }

  private void dumpActive() {
    String active = "";
    for (var entry : this.coordination.entrySet()) {
      active += " " + entry.getKey() + " -> " + entry.getValue() + "\n";
    }
    logger.debug("coordinating on missing sequence state: \n %s ", active);
    this.promised.dumpActive();
  }

  @Override
  public synchronized Optional<OAcceptResult> receiveRetry(OTransactionIdPromise promise) {
    if (promised.isPromised(promise.getId())) {
      ValidationResult result = sequenceManager.validate(promise);
      return switch (result) {
        case VALID -> {
          //          this.promised.addPromised(message);
          yield Optional.empty();
        }
        case ALREADY_PRESENT -> {
          // Already present ... maybe do nothing, already done
          long current = sequenceManager.debugGetSequence(promise.getId().getPosition());
          yield Optional.of(new OInvalidSequential(current, promise.getId().getSequence()));
        }
        case ALREADY_PROMISED -> {
          // Fail for promised to someone else this track it anyway in case of minority in quorum
          //          this.promised.addNotPromised(message);
          yield Optional.of(
              new OAlreadyPromised(
                  sequenceManager.promised(promise.getId().getPosition()).getCoordinator()));
        }
        case MISSING_PREVIOUS -> {
          // wait for previous one, track it anyway
          //          this.promised.addNotPromised(message);
          long current = sequenceManager.debugGetSequence(promise.getId().getPosition());
          yield Optional.of(new OInvalidSequential(current, promise.getId().getSequence()));
        }
      };
    }
    // This should use a different method where it actually check that it was promised successfully
    return Optional.empty();
  }

  public synchronized Optional<OAcceptResult> receive(ODistributedMessage message) {
    OTransactionIdPromise promise = message.getPromiseId();
    ValidationResult result = sequenceManager.validate(promise);
    OTransactionId txId = promise.getId();
    return switch (result) {
      case VALID -> {
        logger.debugNode(topology.getNodeId(), "promising %s", promise);
        this.promised.addPromised(message);
        yield Optional.empty();
      }
      case ALREADY_PRESENT -> {
        // Already present ... maybe do nothing, already done
        long current = sequenceManager.debugGetSequence(txId.getPosition());
        yield Optional.of(new OInvalidSequential(current, txId.getSequence()));
      }
      case ALREADY_PROMISED -> {
        // Fail for promised to someone else this track it anyway in case of minority in quorum
        this.promised.addNotPromised(message);
        yield Optional.of(
            new OAlreadyPromised(sequenceManager.promised(txId.getPosition()).getCoordinator()));
      }
      case MISSING_PREVIOUS -> {
        // wait for previous one, track it anyway
        this.promised.addNotPromised(message);
        long current = sequenceManager.debugGetSequence(txId.getPosition());
        yield Optional.of(new OInvalidSequential(current, txId.getSequence()));
      }
    };
  }

  @Override
  public synchronized void cancelPromise(OTransactionIdPromise promise) {
    boolean promised = sequenceManager.notifyFailure(promise);
    if (promised) {
      logger.debugNode(topology.getNodeId(), "removing promise %s", promise);
      var message = this.promised.removePromised(promise);
      if (message.isPresent()) {
        this.promised.addNotPromised(message.get());
      }
    }
  }

  public synchronized Optional<ODistributedMessage> consensusFailure(
      OTransactionIdPromise promise) {
    boolean promised = sequenceManager.notifyFailure(promise);
    if (promised) {
      logger.debugNode(topology.getNodeId(), "canceling promise %s", promise);
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
  public Optional<OAcceptResult> validateRegisterNode(
      ONodeId node, long version, OTransactionIdPromise promise) {
    return topology.promiseRegister(node, version, promise);
  }

  @Override
  public Set<ONodeId> establish(
      OGroupId groupId, Set<ONodeId> candidates, OTransactionIdPromise promise) {
    Set<ONodeId> result = this.topology.finalizeEstablish(groupId, candidates, promise);
    notifyUpdate();
    return result;
  }

  @Override
  public Optional<OAcceptResult> validateEstablish(
      OGroupId groupId, Set<ONodeId> candidates, OTransactionIdPromise promise) {
    return this.topology.validateEstablish(groupId, candidates, promise);
  }

  @Override
  public synchronized Optional<OTransactionIdPromise> startEstablish(
      Set<ONodeId> nodes, OCompleteAction action) {
    Optional<OTransactionIdPromise> prom = this.sequenceManager.next();
    if (prom.isPresent()) {
      coordination.put(
          prom.get(), action.newResponseCollector(prom.get(), topology.getQuorum(), nodes));
    } else {
      dumpActive();
    }
    return prom;
  }

  @Override
  public synchronized ODiscoverAction nodeJoinStart(
      ONodeId node, ONodeStateNetwork state, boolean merge) {
    var action = this.topology.nodeJoinStart(node, state.topology(), merge);
    action = action.checkAndApply(this, state, merge);
    return action;
  }

  @Override
  public synchronized ONodeStateNetwork getNetworkState() {
    return new ONodeStateNetwork(
        this.topology.getNetworkState(),
        this.databaseTopology.getNetworkState(),
        this.sequenceManager.currentStatus());
  }

  public synchronized void load(ONodeStateStore state) {
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
  public void cancelRegisterNode(OTransactionIdPromise promise) {
    this.topology.cancelRegisterPromise(promise);
  }

  @Override
  public void cancelEstablish(OTransactionIdPromise promise) {
    this.topology.cancelEstablish(promise);
  }

  public Optional<OAcceptResult> validateDeclareDatabase(
      OTransactionIdPromise promise,
      ODatabaseId databaseId,
      String database,
      Set<OAddNodeInfo> partecipants,
      int minimumQuorum) {
    if (!partecipants.stream().anyMatch((x) -> x.node().equals(topology.getNodeId()))) {
      return Optional.of(new OMissingNode(topology.getNodeId()));
    }
    return this.databaseTopology.validateDeclare(
        promise, databaseId, database, partecipants, minimumQuorum);
  }

  public void declareDatabase(
      OTransactionIdPromise promise,
      ODatabaseId dbId,
      String database,
      Set<OAddNodeInfo> partecipants,
      int minimumQuorum) {
    this.databaseTopology.declareDatabase(promise, dbId, database, partecipants, minimumQuorum);
    notifyUpdate();
  }

  public void cancelDeclareDatabase(
      OTransactionIdPromise promise, ODatabaseId dbId, String database) {
    this.databaseTopology.cancelPomise(promise, dbId, database);
  }

  public synchronized long nextTopologyVersion() {
    return this.topology.nextVersion();
  }

  public synchronized long nextDatabaseVersion(ODatabaseId Id) {
    return this.databaseTopology.getDatabaseVersion(Id) + 1;
  }

  public ODatabasesTopologyState getDatabaseTopology() {
    return databaseTopology;
  }

  @Override
  public Optional<OAcceptResult> validateAddDatabaseMember(
      ODatabaseId dbId, List<OAddNodeInfo> nodes, long version, OTransactionIdPromise promise) {
    return this.databaseTopology.validateAddMember(dbId, nodes, version, promise);
  }

  @Override
  public void addDatabaseMember(
      ODatabaseId dbId, List<OAddNodeInfo> nodes, long version, OTransactionIdPromise promise) {
    this.databaseTopology.addDatabaseMember(dbId, nodes, version, promise);
    notifyUpdate();
  }

  @Override
  public void cancelAddDatabaseMember(
      ODatabaseId dbId, List<OAddNodeInfo> nodes, OTransactionIdPromise promise) {
    this.databaseTopology.cancelAddDatabaseMember(dbId, nodes, promise);
  }

  @Override
  public Optional<OAcceptResult> validateRemoveDatabaseMembers(
      ODatabaseId dbId, List<ONodeId> nodes, OVersion version, OTransactionIdPromise promise) {
    return this.databaseTopology.validateRemoveMembers(dbId, nodes, version, promise);
  }

  @Override
  public void removeDatabaseMembers(
      ODatabaseId dbId, List<ONodeId> nodes, OVersion version, OTransactionIdPromise promise) {}

  @Override
  public void cancelRemoveDatabaseMembers(
      ODatabaseId dbId, List<ONodeId> nodes, OTransactionIdPromise promise) {}

  @Override
  public Optional<OAcceptResult> validateSetState(
      ODatabaseId dbId,
      ONodeId nodeId,
      ODatabaseState state,
      long version,
      OTransactionIdPromise promise) {
    return this.databaseTopology.validateSetState(dbId, nodeId, state, version, promise);
  }

  @Override
  public void setState(
      ODatabaseId db,
      ONodeId node,
      ODatabaseState state,
      long version,
      OTransactionIdPromise promise) {
    this.databaseTopology.setState(db, node, state, version, promise);
  }

  @Override
  public void cancelSetState(
      ODatabaseId dbId, ONodeId nodeId, long version, OTransactionIdPromise promise) {
    this.databaseTopology.cancelSetState(dbId, nodeId, version, promise);
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

  public void notifyUpdate() {
    this.updateLister.update(getStore());
  }

  public OTransactionSequenceManager getSequenceManager() {
    return sequenceManager;
  }

  @Override
  public synchronized Optional<OAcceptResult> validateMerge(
      OGroupId group, OTransactionIdPromise promise) {
    return topology.acceptMerge(group, promise);
  }

  @Override
  public synchronized void cancelMerge(OTransactionIdPromise promise) {
    topology.cancelMerge(promise);
  }

  @Override
  public void confirmMerge(
      ONodeId node, OTransactionIdPromise promise, Optional<OAcceptResult> accepted) {
    if (accepted.isEmpty()) {
      nodeSuccess(node, promise);
    } else {
      nodeFailure(node, promise, accepted.get());
    }
  }

  public OGroupId getGroupId() {
    return topology.getGroupId();
  }

  @Override
  public ODisconnectAction nodeDisconnected(ONodeId node) {
    List<CompleteInfo> actions = disconnectCoordinated(node);
    for (var action : actions) {
      action.action().failure(action.promise(), action.nodes(), action.result());
    }
    return this.promised.nodeDisconnected(node);
  }

  @Override
  public OTransactionSequenceStatus getTransactionSequenceState() {
    return sequenceManager.currentStatus();
  }

  @Override
  public synchronized void receivePing(ONodeId nodeId, OTransactionSequenceStatus status) {
    this.topology.ping(nodeId);
  }

  @Override
  public synchronized Set<ONodeId> checkOffline(long time) {
    return this.topology.awayNodes(time);
  }

  @Override
  public ONodeId getCurrent() {
    return topology.getNodeId();
  }

  @Override
  public int getDatabaseQuorum(ODatabaseId databaseId) {
    return this.databaseTopology.getQuorum(databaseId);
  }

  @Override
  public Optional<OAcceptResult> validateDropDatabase(
      ODatabaseId dbId, OVersion version, OTransactionIdPromise promise) {
    return this.databaseTopology.validateDropDatabase(promise, dbId, version);
  }

  @Override
  public void dropDatabase(ODatabaseId dbId, OVersion version, OTransactionIdPromise promise) {
    this.databaseTopology.dropDatabase(promise, dbId, version);
  }

  @Override
  public void cancelDropDatabase(
      ODatabaseId dbId, OVersion version, OTransactionIdPromise promise) {
    this.databaseTopology.cancelDropDatabase(dbId, version, promise);
  }
}
