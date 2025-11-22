package com.orientechnologies.orient.distributed.context;

import com.orientechnologies.orient.core.transaction.OGroupId;
import com.orientechnologies.orient.core.transaction.ONodeId;
import com.orientechnologies.orient.core.transaction.OTransactionIdPromise;
import com.orientechnologies.orient.distributed.context.OResponseCollector.CompleteInfo;
import com.orientechnologies.orient.distributed.context.coordination.message.OTopologyStateNetwork;
import com.orientechnologies.orient.distributed.context.coordination.result.OAcceptResult;
import com.orientechnologies.orient.distributed.context.topology.ODiscoverAction;
import com.orientechnologies.orient.distributed.context.topology.OTopologyManager;
import com.orientechnologies.orient.server.distributed.ODistributedException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public class OCoordinatedDistributedOpsImpl implements OCoordinatedDistributedOps {

  private OTopologyManager topology;

  private final Map<OTransactionIdPromise, OResponseCollector> coordination = new HashMap<>();

  public OCoordinatedDistributedOpsImpl(ONodeId current, OGroupId groupId, int quorum) {
    topology = new OTopologyManager(current, groupId, quorum);
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
  public synchronized OOperationStart start(OTransactionIdPromise promise, OCompleteAction action) {
    if (this.topology.enoughNodes()) {
      throw new ODistributedException(
          String.format(
              "No enough nodes to coordinate an opertion with quorum: %d know nodes:%s",
              this.topology.getMinimumQuorum(), this.getMembers().toString()));
    }
    Set<ONodeId> nodes = Collections.unmodifiableSet(new HashSet<>(topology.getMembers()));
    coordination.put(promise, new OResponseCollector(action, promise, topology.getQuorum(), nodes));
    return new OOperationStart(promise, nodes);
  }

  @Override
  public void success(ONodeId node, OTransactionIdPromise promise) {
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
  public void failure(ONodeId node, OTransactionIdPromise promise, OAcceptResult acceptResult) {
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
  public boolean promiseRegister(ONodeId node, long version) {
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
  public void startEstablish(
      OTransactionIdPromise idPromise, Set<ONodeId> nodes, OCompleteAction action) {
    coordination.put(
        idPromise, new OResponseCollector(action, idPromise, topology.getQuorum(), nodes));
  }

  @Override
  public ODiscoverAction nodeJoinStart(ONodeId node, OTopologyStateNetwork state) {
    return this.topology.nodeJoinStart(node, state);
  }

  @Override
  public OTopologyStateNetwork getNetworkState() {
    return this.topology.getNetworkState();
  }

  @Override
  public void load(ONodeStateStore nodeStateStore) {
    this.topology.load(nodeStateStore);
  }

  @Override
  public void cancelRegisterPromise() {
    this.topology.cancelRegisterPromise();
  }

  @Override
  public void cancelEnstablish() {
    this.topology.cancelEnstablish();
  }
}
