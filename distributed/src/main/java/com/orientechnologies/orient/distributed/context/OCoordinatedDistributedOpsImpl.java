package com.orientechnologies.orient.distributed.context;

import com.orientechnologies.orient.core.transaction.ODatabaseId;
import com.orientechnologies.orient.core.transaction.OGroupId;
import com.orientechnologies.orient.core.transaction.ONodeId;
import com.orientechnologies.orient.core.transaction.OTransactionIdPromise;
import com.orientechnologies.orient.core.transaction.OTransactionSequenceManager;
import com.orientechnologies.orient.core.tx.OTransactionSequenceStatus;
import com.orientechnologies.orient.core.tx.ValidationResult;
import com.orientechnologies.orient.distributed.context.OResponseCollector.CompleteInfo;
import com.orientechnologies.orient.distributed.context.coordination.message.ODistributedMessage;
import com.orientechnologies.orient.distributed.context.coordination.message.ONodeStateNetwork;
import com.orientechnologies.orient.distributed.context.coordination.message.operation.OAddNodeInfo;
import com.orientechnologies.orient.distributed.context.coordination.result.OAcceptResult;
import com.orientechnologies.orient.distributed.context.coordination.result.OAlreadyPromised;
import com.orientechnologies.orient.distributed.context.coordination.result.OInvalidSequential;
import com.orientechnologies.orient.distributed.context.coordination.result.OMissingNode;
import com.orientechnologies.orient.distributed.context.topology.ODiscoverAction;
import com.orientechnologies.orient.distributed.context.topology.OTopologyManager;
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

  public OCoordinatedDistributedOpsImpl(
      ONodeId current, OGroupId groupId, int quorum, ODatabaseStateChangeListener listener) {
    topology = new OTopologyManager(current, groupId, quorum);
    sequenceManager = new OTransactionSequenceManager(current, 3);
    promised = new OPromisedDistributedOpsImpl();
    coordination = new HashMap<>();
    this.databaseTopology = new ODatabasesTopologyState(listener);
  }

  @Override
  public ODiscoverAction discoverNode(ONodeId node) {
    return this.topology.nodeDiscovered(node);
  }

  public synchronized void registerNode(ONodeId node, long version) {
    this.topology.register(node, version);
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
              "No enough nodes to coordinate an opertion with quorum: %d know nodes:%s",
              this.topology.getMinimumQuorum(), this.getMembers().toString()));
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

  public Optional<OAcceptResult> receive(ODistributedMessage message) {
    ValidationResult result = sequenceManager.validate(message.getPromiseId());
    switch (result) {
      case VALID -> {
        this.promised.addPromised(message);
        return Optional.empty();
      }
      case ALREADY_PRESENT -> {
        // Already present ... maybe do nothing, already done
        long current =
            sequenceManager.debugGetSequence(message.getPromiseId().getId().getPosition());
        return Optional.of(
            new OInvalidSequential(current, message.getPromiseId().getId().getSequence()));
      }
      case ALREADY_PROMISED -> {
        // Fail for promised to someone else this track it anyway in case of minority in quorum
        this.promised.addNotPromised(message);
        return Optional.of(new OAlreadyPromised());
      }
      case MISSING_PREVIOUS -> {
        // wait for previous one, track it anyway
        this.promised.addNotPromised(message);
        long current =
            sequenceManager.debugGetSequence(message.getPromiseId().getId().getPosition());
        return Optional.of(
            new OInvalidSequential(current, message.getPromiseId().getId().getSequence()));
      }
    }
    long current = sequenceManager.debugGetSequence(message.getPromiseId().getId().getPosition());
    return Optional.of(
        new OInvalidSequential(current, message.getPromiseId().getId().getSequence()));
  }

  public Optional<ODistributedMessage> consensusFailure(OTransactionIdPromise promise) {
    boolean promised = sequenceManager.notifyFailure(promise);
    if (promised) {
      return this.promised.removePromised(promise);
    }
    return Optional.empty();
  }

  public Optional<ODistributedMessage> consensusSuccess(OTransactionIdPromise promise) {
    // TODO: if received the confirmation for not promised message have to cancel eventual
    // promised message and try to promised and apply currently confirmed message.
    ValidationResult result = sequenceManager.notifySuccess(promise);
    switch (result) {
      case VALID -> {
        ODistributedMessage message = this.promised.getPromised(promise);
        return Optional.ofNullable(message);
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

    return Optional.empty();
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
      // This do not call the complete with result, because it will be left to the
      // post execution call to completeExecution
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
  public Set<ONodeId> getMembers() {
    return topology.getMembers();
  }

  @Override
  public Optional<OAcceptResult> validateRegisterNode(ONodeId node, long version) {
    return topology.promiseRegister(node, version);
  }

  @Override
  public Set<ONodeId> enstablish(OGroupId groupId, Set<ONodeId> candidates) {
    return this.topology.finalizeEnstablish(groupId, candidates);
  }

  @Override
  public Optional<OAcceptResult> validateEnstablish(OGroupId groupId, Set<ONodeId> candidates) {
    return this.topology.validateEnstablish(groupId, candidates);
  }

  @Override
  public Optional<OTransactionIdPromise> startEstablish(
      Set<ONodeId> nodes, OCompleteAction action) {
    Optional<OTransactionIdPromise> prom = this.sequenceManager.next();
    if (prom.isPresent()) {
      coordination.put(
          prom.get(), new OResponseCollector(action, prom.get(), topology.getQuorum(), nodes));
    }
    return prom;
  }

  @Override
  public ODiscoverAction nodeJoinStart(ONodeId node, ONodeStateNetwork state) {
    var action = this.topology.nodeJoinStart(node, state.getTopology());
    if (action.applyDatabaseState()) {
      if (state.getTopology().isMerge()) {
        this.databaseTopology.mergeNetworkState(state.getDatabases());
      } else {
        this.databaseTopology.receiverNetworkState(state.getDatabases());
      }
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

  public void load(
      Optional<ONodeStateStore> nodeStateStore, Optional<OTransactionSequenceStatus> status) {
    if (nodeStateStore.isPresent()) {
      this.topology.load(nodeStateStore.get());
    }
    if (status.isPresent()) {
      this.sequenceManager.fill(status.get());
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

  public void cancelDeclareDatabase(
      OTransactionIdPromise promise, ODatabaseId dbId, String database) {
    this.databaseTopology.cancelPomise(promise, dbId, database);
  }

  public ODatabasesTopologyState getDatabaseTopology() {
    return databaseTopology;
  }

  public Optional<OAcceptResult> validateAddDatabaseMember(
      ODatabaseId dbId, List<OAddNodeInfo> nodes, long version) {
    return this.databaseTopology.promiseAddMember(dbId, nodes, version);
  }

  public void addDatabaseMember(ODatabaseId dbId, List<OAddNodeInfo> nodes, long version) {
    this.databaseTopology.addDatabaseMember(dbId, nodes, version);
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
}
