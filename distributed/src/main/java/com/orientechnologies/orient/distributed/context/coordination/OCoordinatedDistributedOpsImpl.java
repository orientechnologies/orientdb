package com.orientechnologies.orient.distributed.context.coordination;

import com.orientechnologies.common.util.ORawPair;
import com.orientechnologies.orient.core.transaction.ODatabaseId;
import com.orientechnologies.orient.core.transaction.OGroupId;
import com.orientechnologies.orient.core.transaction.ONodeId;
import com.orientechnologies.orient.core.transaction.OTransactionId;
import com.orientechnologies.orient.core.transaction.OTransactionIdPromise;
import com.orientechnologies.orient.core.transaction.OTransactionSequenceManager;
import com.orientechnologies.orient.core.tx.OTransactionSequenceStatus;
import com.orientechnologies.orient.core.tx.SuccessResult;
import com.orientechnologies.orient.core.tx.ValidationResult;
import com.orientechnologies.orient.distributed.context.ONodeStateStore;
import com.orientechnologies.orient.distributed.context.ONodeStateUpdated;
import com.orientechnologies.orient.distributed.context.coordination.OResponseCollector.CompleteInfo;
import com.orientechnologies.orient.distributed.context.coordination.action.OCompleteAction;
import com.orientechnologies.orient.distributed.context.coordination.dbs.OCanSyncResult;
import com.orientechnologies.orient.distributed.context.coordination.dbs.ODatabaseState;
import com.orientechnologies.orient.distributed.context.coordination.dbs.ODatabaseStateChangeListener;
import com.orientechnologies.orient.distributed.context.coordination.dbs.ODatabasesTopologyState;
import com.orientechnologies.orient.distributed.context.coordination.dbs.ONodeRole;
import com.orientechnologies.orient.distributed.context.coordination.dbs.ONotificationAction;
import com.orientechnologies.orient.distributed.context.coordination.message.OCanSyncAccept;
import com.orientechnologies.orient.distributed.context.coordination.message.ODistributedMessage;
import com.orientechnologies.orient.distributed.context.coordination.message.operation.OAddNodeInfo;
import com.orientechnologies.orient.distributed.context.coordination.message.state.ODatabaseMemberNetwork;
import com.orientechnologies.orient.distributed.context.coordination.message.state.ODatabaseStateNetwork;
import com.orientechnologies.orient.distributed.context.coordination.message.state.ONodeStateNetwork;
import com.orientechnologies.orient.distributed.context.coordination.message.state.OTopologyStateNetwork;
import com.orientechnologies.orient.distributed.context.coordination.result.OAcceptResult;
import com.orientechnologies.orient.distributed.context.coordination.result.OAlreadyPromised;
import com.orientechnologies.orient.distributed.context.coordination.result.OCannotMerge;
import com.orientechnologies.orient.distributed.context.coordination.result.ODatabaseMissing;
import com.orientechnologies.orient.distributed.context.coordination.result.OInvalidSequential;
import com.orientechnologies.orient.distributed.context.coordination.result.OMissingNode;
import com.orientechnologies.orient.distributed.context.coordination.result.OOutdatedVersion;
import com.orientechnologies.orient.distributed.context.coordination.sync.OSyncId;
import com.orientechnologies.orient.distributed.context.coordination.sync.OSyncInfo;
import com.orientechnologies.orient.distributed.context.coordination.sync.OSyncState;
import com.orientechnologies.orient.distributed.context.coordination.topology.ODiscoverAction;
import com.orientechnologies.orient.distributed.context.coordination.topology.OTopologyManager;
import com.orientechnologies.orient.distributed.context.coordination.topology.OTopologyState;
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
  private Optional<ORawPair<OTransactionIdPromise, ONodeStateNetwork>> promisedMergeState =
      Optional.empty();

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

  public synchronized void registerNode(
      ONodeId node, OVersion version, OTransactionIdPromise promise) {
    this.topology.register(node, version, promise);
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
      logger.error(
          "No enough nodes to coordinate operation: %s with quorum: %d know nodes:%s",
          null,
          action.toString(),
          this.topology.getMinimumQuorum(),
          this.topology.getMembers().toString());
      return Optional.empty();
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
              this.topology.getMinimumQuorum(), this.topology.getMembers().toString()));
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
          yield Optional.of(new OInvalidSequential(current + 1, promise.getId().getSequence()));
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
          yield Optional.of(new OInvalidSequential(current + 1, promise.getId().getSequence()));
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
        yield Optional.of(new OInvalidSequential(current + 1, txId.getSequence()));
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
        yield Optional.of(new OInvalidSequential(current + 1, txId.getSequence()));
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

  public synchronized OConsensusSuccess consensusSuccess(OTransactionIdPromise promise) {
    SuccessResult result = sequenceManager.notifySuccess(promise);
    return switch (result) {
      case VALID -> {
        yield new OConsensusSuccess(this.promised.getPromised(promise));
      }
      case VALID_MISSING -> {
        yield new OConsensusSuccess(this.promised.getNotPromised(promise), true);
      }
      case ALREADY_PRESENT -> {
        // Already present ... maybe do nothing, already done
        finalize(promise);
        yield new OConsensusSuccess();
      }
      case ALREADY_PROMISED -> {
        // Consensus has been reached on a different message of what the current node promised,
        // cancel the promised and try to promise e apply the message that reached consensus
        var promisedPromise = this.sequenceManager.promised(promise.getId().getPosition());
        yield new OConsensusSuccess(promisedPromise);
      }
      case MISSING_PREVIOUS -> {
        // wait for previous one
        yield new OConsensusSuccess();
      }
    };
  }

  private synchronized void finalize(OTransactionIdPromise promise) {
    this.promised.finalize(promise);
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
    this.notifyUpdate();
  }

  @Override
  public Optional<OAcceptResult> validateRegisterNode(
      ONodeId node, OVersion version, OTransactionIdPromise promise) {
    return topology.promiseRegister(node, version, promise);
  }

  @Override
  public Set<ONodeId> establish(
      OGroupId groupId, Set<ONodeId> candidates, OTransactionIdPromise promise) {
    Set<ONodeId> result = this.topology.finalizeEstablish(groupId, candidates, promise);
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
    if (action.loadDatabasesData()) {
      this.databaseTopology.receiverNetworkState(state.databases());
    }
    if (action.loadSequenceData()) {
      this.sequenceManager.fill(state.sequenceStatus());
    }

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
  }

  public void cancelDeclareDatabase(
      OTransactionIdPromise promise, ODatabaseId dbId, String database) {
    this.databaseTopology.cancelPomise(promise, dbId, database);
  }

  public synchronized OVersion nextTopologyVersion() {
    return this.topology.nextVersion();
  }

  public synchronized OVersion nextDatabaseVersion(ODatabaseId Id) {
    return this.databaseTopology.getDatabaseVersion(Id).next();
  }

  public ODatabasesTopologyState getDatabaseTopology() {
    return databaseTopology;
  }

  @Override
  public ONetworkTopology getNetworkTopology() {
    return topology;
  }

  @Override
  public Optional<OAcceptResult> validateAddDatabaseMember(
      ODatabaseId dbId, List<OAddNodeInfo> nodes, OVersion version, OTransactionIdPromise promise) {
    return this.databaseTopology.validateAddMember(dbId, nodes, version, promise);
  }

  @Override
  public void addDatabaseMember(
      ODatabaseId dbId, List<OAddNodeInfo> nodes, OVersion version, OTransactionIdPromise promise) {
    this.databaseTopology.addDatabaseMember(dbId, nodes, version, promise);
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
      ODatabaseId dbId, List<ONodeId> nodes, OVersion version, OTransactionIdPromise promise) {
    this.databaseTopology.removeDatabaseMembers(dbId, nodes, version, promise);
  }

  @Override
  public void cancelRemoveDatabaseMembers(
      ODatabaseId dbId, List<ONodeId> nodes, OTransactionIdPromise promise) {
    this.databaseTopology.cancelRemoveDatabaseMembers(dbId, promise);
  }

  @Override
  public Optional<OAcceptResult> validateSetTopologyQuorum(
      int quorum, OVersion version, OTransactionIdPromise promise) {
    return this.topology.validateSetQuorum(quorum, version, promise);
  }

  @Override
  public void setTopologyQuorum(int quorum, OVersion version, OTransactionIdPromise promise) {
    this.topology.setQuorum(quorum, version, promise);
  }

  @Override
  public void cancelSetTopologyQuorum(int quorum, OVersion version, OTransactionIdPromise promise) {
    this.topology.cancelSetQuorum(quorum, version, promise);
  }

  @Override
  public Optional<OAcceptResult> validateSetState(
      ODatabaseId dbId,
      ONodeId nodeId,
      ODatabaseState state,
      OVersion version,
      OTransactionIdPromise promise) {
    return this.databaseTopology.validateSetState(dbId, nodeId, state, version, promise);
  }

  @Override
  public void setState(
      ODatabaseId db,
      ONodeId node,
      ODatabaseState state,
      OVersion version,
      OTransactionIdPromise promise) {
    this.databaseTopology.setState(db, node, state, version, promise);
  }

  @Override
  public void cancelSetState(
      ODatabaseId dbId, ONodeId nodeId, OVersion version, OTransactionIdPromise promise) {
    this.databaseTopology.cancelSetState(dbId, nodeId, version, promise);
  }

  @Override
  public Optional<OSyncInfo> newSync(ODatabaseId dbId) {
    return this.databaseTopology.newSync(dbId);
  }

  @Override
  public boolean acceptSync(ONodeId sender, OSyncId syncId) {
    return this.databaseTopology.acceptSync(sender, syncId);
  }

  @Override
  public Optional<OCanSyncResult> canSync(ONodeId sender, OSyncId syncId, OCanSyncAccept canSync) {
    return this.databaseTopology.canSync(sender, syncId, canSync);
  }

  @Override
  public Optional<OSyncState> startSend(ONodeId from, OSyncId syncId, OCanSyncAccept mode) {
    return this.databaseTopology.startSend(from, syncId, mode);
  }

  @Override
  public void requestNext(OSyncId syncId, boolean close) {
    this.databaseTopology.requestNext(syncId, close);
  }

  @Override
  public Optional<OSyncState> getSyncState(OSyncId syncId) {
    return this.databaseTopology.getSyncState(syncId);
  }

  public boolean executeOnOneOnline(ODatabaseId dbId, ONotificationAction execute) {
    return this.databaseTopology.executeOnOneOnline(dbId, execute);
  }

  @Override
  public boolean waitOnlineQuorum(ODatabaseId dbId, Optional<Long> timeout)
      throws InterruptedException {
    return this.databaseTopology.waitOnlineQuorum(dbId, timeout);
  }

  @Override
  public boolean waitOnlineAll(ODatabaseId dbId, Optional<Long> timeout)
      throws InterruptedException {
    return this.databaseTopology.waitOnlineAll(dbId, timeout);
  }

  @Override
  public boolean waitSelfOnline(ODatabaseId dbId, Optional<Long> timeout)
      throws InterruptedException {
    return this.databaseTopology.waitSelfOnline(dbId, timeout);
  }

  @Override
  public boolean waitSelfOnline(String dbName, Optional<Long> timeout) throws InterruptedException {
    return this.databaseTopology.waitSelfOnline(dbName, timeout);
  }

  @Override
  public boolean isApplied(OTransactionId txId) {
    return this.sequenceManager.isApplied(txId);
  }

  @Override
  public void completeSync(OSyncId syncId, boolean success) {
    this.databaseTopology.completeSync(syncId, success);
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
  public synchronized Optional<OAcceptResult> validateMergeToNetwork(
      OGroupId group,
      ONodeStateNetwork state,
      ONodeStateNetwork original,
      OTransactionIdPromise promise) {
    var res = topology.validateMergeToNetwork(group, original, promise);
    if (res.isEmpty()) {
      res = this.databaseTopology.validateMergeToNetwork(original.databases(), promise);
      if (res.isEmpty()) {
        this.promisedMergeState = Optional.of(new ORawPair<>(promise, state));
      } else {
        topology.cancelMerge(promise);
      }
    }
    if (res.isPresent()) {
      OAcceptResult acc = res.get();
      if (acc instanceof OInvalidSequential
          || acc instanceof ODatabaseMissing
          || acc instanceof OOutdatedVersion) {
        return Optional.of(new OCannotMerge(getNetworkState(), acc));
      }
    }
    return res;
  }

  @Override
  public synchronized void cancelMergeToNetwork(OTransactionIdPromise promise) {
    if (this.promisedMergeState.isPresent()
        && this.promisedMergeState.get().first.equals(promise)) {
      topology.cancelMerge(promise);
      databaseTopology.cancelMerge(promise);
      promisedMergeState = Optional.empty();
      logger.debugNode(this.getCurrent(), "canceling merging network for promise %s", promise);
    } else {
      logger.debugNode(this.getCurrent(), "ignoring cancel merge for promise %s", promise);
    }
  }

  @Override
  public synchronized void mergeToNetwork(OTransactionIdPromise promise) {
    logger.debugNode(this.getCurrent(), "merging network for promise %s", promise);
    if (this.promisedMergeState.isPresent()
        && this.promisedMergeState.get().first.equals(promise)) {
      var state = this.promisedMergeState.get().second;
      topology.applyMerge(state.topology(), promise);
      this.databaseTopology.mergeNetworkState(state.databases(), promise);
      this.promisedMergeState = Optional.empty();
      this.sequenceManager.fill(state.sequenceStatus());
      this.sequenceManager.notifySuccess(promise);
    } else {
      logger.warnNode(
          this.getCurrent(),
          "received merge confirmation for %s promised merge: %s ",
          promise,
          this.promisedMergeState);
    }
  }

  @Override
  public void nodeMergeResult(
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
    this.databaseTopology.nodeDisconnected(node);
    return this.promised.nodeDisconnected(node);
  }

  @Override
  public OTransactionSequenceStatus getTransactionSequenceState() {
    return sequenceManager.currentStatus();
  }

  @Override
  public synchronized List<OTransactionId> receivePing(
      ONodeId nodeId, OTransactionSequenceStatus status) {
    this.topology.ping(nodeId);
    return this.sequenceManager.checkSelfStatus(status);
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
  public synchronized void dropDatabase(
      ODatabaseId dbId, OVersion version, OTransactionIdPromise promise) {
    this.databaseTopology.dropDatabase(promise, dbId, version);
    notifyUpdate();
  }

  @Override
  public void cancelDropDatabase(
      ODatabaseId dbId, OVersion version, OTransactionIdPromise promise) {
    this.databaseTopology.cancelDropDatabase(dbId, version, promise);
  }

  @Override
  public synchronized Optional<OAcceptResult> validateMergeNode(
      ONodeId node,
      ONodeStateNetwork state,
      ONodeStateNetwork original,
      OTransactionIdPromise promise) {
    var accepted = this.topology.validateMergeNode(state.topology().groupId(), original, promise);
    if (accepted.isEmpty()) {
      accepted = this.databaseTopology.validateMergeNode(original.databases(), promise);
      if (accepted.isPresent()) {
        this.topology.cancelRegisterPromise(promise);
      }
    }
    return Optional.empty();
  }

  @Override
  public synchronized void mergeNode(
      ONodeId node,
      ONodeStateNetwork state,
      ONodeStateNetwork original,
      OTransactionIdPromise promise) {
    this.topology.mergeNode(node, state.topology().version(), promise);
    this.databaseTopology.mergeNetworkState(state.databases(), promise);
    this.sequenceManager.fill(state.sequenceStatus());
  }

  @Override
  public synchronized void cancelMergeNode(
      ONodeId node,
      ONodeStateNetwork state,
      ONodeStateNetwork original,
      OTransactionIdPromise promise) {
    this.topology.cancelRegisterPromise(promise);
    this.databaseTopology.cancelMerge(promise);
  }

  @Override
  public synchronized Optional<ONodeStateNetwork> createMergedState(ONodeStateNetwork state) {
    return mergeState(state, getNetworkState());
  }

  private Optional<ONodeStateNetwork> mergeState(
      ONodeStateNetwork state, ONodeStateNetwork networkState) {
    var topology = mergeTopology(state.topology(), networkState.topology());
    var databases = mergeDatabases(state.databases(), networkState.databases(), topology.members());
    if (databases.isEmpty()) {
      return Optional.empty();
    }
    var sequence = mergeSequence(state.sequenceStatus(), networkState.sequenceStatus());
    return Optional.of(new ONodeStateNetwork(topology, databases.get(), sequence));
  }

  private OTransactionSequenceStatus mergeSequence(
      OTransactionSequenceStatus sequenceStatus, OTransactionSequenceStatus sequenceStatus2) {
    long[] status = sequenceStatus.getStatus();
    int size = status.length;
    long[] status2 = sequenceStatus2.getStatus();
    if (status2.length > size) {
      size = status2.length;
    }
    long newStatus[] = new long[size];
    for (int i = 0; i < size; i++) {
      long st = 0;
      if (i < status.length) {
        st = status[i];
      }
      if (i < status2.length && st < status2[i]) {
        st = status2[i];
      }
      newStatus[i] = st + 1;
    }
    return new OTransactionSequenceStatus(newStatus);
  }

  private Optional<List<ODatabaseStateNetwork>> mergeDatabases(
      List<ODatabaseStateNetwork> databases,
      List<ODatabaseStateNetwork> databases2,
      Set<ONodeId> allNetwork) {
    Map<ODatabaseId, ODatabaseStateNetwork> dbs = new HashMap<>();
    Map<String, ODatabaseStateNetwork> dbNames = new HashMap<>();
    for (var db : databases) {
      dbs.put(db.id(), db);
      dbNames.put(db.name(), db);
    }
    for (var db : databases2) {
      var db1 = dbs.get(db.id());
      if (db1 != null) {
        dbs.put(db.id(), mergeDatabase(db1, db, allNetwork));
      } else if (dbNames.containsKey(db.name())) {
        return Optional.empty();
      } else {
        var members = new ArrayList<>(db.members());
        for (ONodeId node : allNetwork) {
          if (db.members().stream().noneMatch((x) -> x.node().equals(node))) {
            // TODO: define the role from a configuration
            members.add(new ODatabaseMemberNetwork(node, ONodeRole.Main, ODatabaseState.Offline));
          }
        }
        int newQuorum = db.quorum();
        int newCompQuorum = (members.size() / 2) + 1;
        if (newCompQuorum >= newQuorum) {
          newQuorum = newCompQuorum;
        }

        dbs.put(
            db.id(),
            new ODatabaseStateNetwork(db.id(), db.name(), newQuorum, db.version().next(), members));
      }
    }
    return Optional.of(new ArrayList<>(dbs.values()));
  }

  private ODatabaseStateNetwork mergeDatabase(
      ODatabaseStateNetwork database, ODatabaseStateNetwork database2, Set<ONodeId> allNetwork) {

    int newQuorum;
    if (database.quorum() > database2.quorum()) {
      newQuorum = database.quorum();
    } else {
      newQuorum = database2.quorum();
    }
    OVersion newVersion;
    if (database.version().isAfter(database2.version())) {
      newVersion = database.version();
    } else {
      newVersion = database2.version();
    }

    // Make sure it will progress version
    newVersion = newVersion.next();

    Map<ONodeId, ODatabaseMemberNetwork> members = new HashMap<>();

    for (ODatabaseMemberNetwork member : database.members()) {
      members.put(member.node(), member);
    }

    for (ODatabaseMemberNetwork member : database2.members()) {
      var om = members.get(member.node());
      if (om != null) {
        members.put(member.node(), mergeMember(om, member));
      } else {
        members.put(member.node(), member);
      }
    }

    for (ONodeId node : allNetwork) {
      if (!members.containsKey(node)) {
        members.put(node, new ODatabaseMemberNetwork(node, ONodeRole.Main, ODatabaseState.Offline));
      }
    }

    int newCompQuorum = (members.size() / 2) + 1;
    if (newCompQuorum >= newQuorum) {
      newQuorum = newCompQuorum;
    }

    return new ODatabaseStateNetwork(
        database.id(), database.name(), newQuorum, newVersion, new ArrayList<>(members.values()));
  }

  private ODatabaseMemberNetwork mergeMember(
      ODatabaseMemberNetwork om, ODatabaseMemberNetwork member) {
    var role = om.role();
    if (ONodeRole.Main.equals(member.role())) {
      role = ONodeRole.Main;
    }
    return new ODatabaseMemberNetwork(om.node(), role, om.state());
  }

  private OTopologyStateNetwork mergeTopology(
      OTopologyStateNetwork topology, OTopologyStateNetwork networkState) {
    var newMembers = new HashSet<ONodeId>(topology.members());
    newMembers.addAll(networkState.members());
    int newQuorum;
    if (topology.quorum() > networkState.quorum()) {
      newQuorum = topology.quorum();
    } else {
      newQuorum = networkState.quorum();
    }
    int newCompQuorum = (newMembers.size() / 2) + 1;
    if (newCompQuorum >= newQuorum) {
      newQuorum = newCompQuorum;
    }
    OVersion newVersion;
    if (topology.version().isAfter(networkState.version())) {
      newVersion = topology.version();
    } else {
      newVersion = networkState.version();
    }
    // Make sure it will progress version
    newVersion = newVersion.next();
    return new OTopologyStateNetwork(
        topology.groupId(), OTopologyState.ESTABLISHED, newMembers, newQuorum, newVersion);
  }

  @Override
  public Optional<OAcceptResult> validateSetDatabaseNodeRole(
      ODatabaseId db,
      ONodeId node,
      ONodeRole role,
      OVersion version,
      OTransactionIdPromise promise) {
    return this.databaseTopology.validateSetDatabaseNodeRole(db, node, role, version, promise);
  }

  @Override
  public void setDatabaseNodeRole(
      ODatabaseId db,
      ONodeId node,
      ONodeRole role,
      OVersion version,
      OTransactionIdPromise promise) {
    this.databaseTopology.setDatabaseNodeRole(db, node, role, version, promise);
  }

  @Override
  public void cancelSetDatabaseNodeRole(
      ODatabaseId db,
      ONodeId node,
      ONodeRole role,
      OVersion version,
      OTransactionIdPromise promise) {
    this.databaseTopology.cancelSetDatabaseNodeRole(db, node, role, version, promise);
  }

  @Override
  public void dbRemovedFromDiskWhenOffline(ODatabaseId db) {
    this.databaseTopology.dbRemovedFromDiskWhenOffline(db);
  }

  public boolean waitForEnstablish(Optional<Long> timeout) throws InterruptedException {
    return this.topology.waitForEnstablished(timeout);
  }

  public void executeOnEnstablish(ONotificationAction action) {
    this.topology.executeOnEnstablish(action);
  }

  public Optional<OAcceptResult> validateSetDatabaseQuorum(
      ODatabaseId db, int quorum, OVersion version, OTransactionIdPromise promise) {
    return this.databaseTopology.validateSetDatabaseQuorum(db, quorum, version, promise);
  }

  public void setDatabaseQuorum(
      ODatabaseId db, int quorum, OVersion version, OTransactionIdPromise promise) {
    this.databaseTopology.setDatabaseQuorum(db, quorum, version, promise);
  }

  public void cancelSetDatabaseQuorum(
      ODatabaseId db, int quorum, OVersion version, OTransactionIdPromise promise) {
    this.databaseTopology.cancelSetDatabaseQuorum(db, quorum, version, promise);
  }

  @Override
  public void checkTimeoutSync(long timeOutSync) {
    this.databaseTopology.checkTimeoutSync(timeOutSync);
  }

  @Override
  public boolean waitNodeJoined(ONodeId dbId, Optional<Long> timeout) throws InterruptedException {
    return this.topology.waitNodeJoined(dbId, timeout);
  }
}
