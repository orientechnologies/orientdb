package com.orientechnologies.orient.distributed.context;

import com.orientechnologies.orient.core.transaction.ONodeId;
import com.orientechnologies.orient.core.transaction.OTransactionIdPromise;
import com.orientechnologies.orient.distributed.context.OResponseCollector.CompleteInfo;
import com.orientechnologies.orient.server.distributed.ODistributedException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public class OCoordinatedDistributedOpsImpl implements OCoordinatedDistributedOps {

  private final Map<OTransactionIdPromise, OResponseCollector> coordination = new HashMap<>();
  private final Set<ONodeId> activeNodes = new HashSet<>();
  private final Set<ONodeId> publicNodes = Collections.unmodifiableSet(activeNodes);
  private final int minQuorum;
  private volatile int quorum;

  public OCoordinatedDistributedOpsImpl(int quorum) {
    this.minQuorum = quorum;
    this.quorum = quorum;
  }

  public synchronized void registerNode(ONodeId node) {
    if (activeNodes.add(node)) {
      int newQuorum = (activeNodes.size() / 2) + 1;
      if (newQuorum >= minQuorum) {
        this.quorum = newQuorum;
      }
    }
  }

  public void unregisterNode(ONodeId node) {
    Optional<CompleteInfo> action = Optional.empty();
    synchronized (this) {
      if (activeNodes.remove(node)) {
        int newQuorum = (activeNodes.size() / 2) + 1;
        if (newQuorum >= minQuorum) {
          this.quorum = newQuorum;
        }
      }
      Iterator<OResponseCollector> iterator = coordination.values().iterator();
      while (iterator.hasNext()) {
        OResponseCollector coll = iterator.next();
        action = coll.disconnected(node);
        if (coll.isFinished()) {
          iterator.remove();
        }
      }
    }
    if (action.isPresent()) {
      // TODO: run in a context
      CompleteInfo info = action.get();
      info.action().failure(info.promise(), info.nodes());
    }
  }

  @Override
  public synchronized Set<ONodeId> start(OTransactionIdPromise promise, OCompleteAction action) {
    if (this.activeNodes.size() < this.minQuorum) {
      throw new ODistributedException(
          String.format(
              "No enough nodes to coordinate an opertion with quorum: %d know nodes:%s",
              this.minQuorum, this.activeNodes.toString()));
    }
    Set<ONodeId> nodes = Collections.unmodifiableSet(new HashSet<>(activeNodes));
    coordination.put(promise, new OResponseCollector(action, promise, quorum, nodes));
    return nodes;
  }

  @Override
  public void success(ONodeId node, OTransactionIdPromise promise) {
    Optional<CompleteInfo> action = Optional.empty();
    synchronized (this) {
      OResponseCollector coll = coordination.get(promise);
      if (coll != null) {
        action = coll.receive(node);
        if (coll.isFinished()) {
          coordination.remove(promise);
        }
      }
    }
    if (action.isPresent()) {
      // TODO: run in a context
      CompleteInfo info = action.get();
      info.action().success(info.promise(), info.nodes());
    }
  }

  @Override
  public void failure(ONodeId node, OTransactionIdPromise promise) {
    Optional<CompleteInfo> action = Optional.empty();
    synchronized (this) {
      OResponseCollector coll = coordination.get(promise);
      if (coll != null) {
        action = coll.fail(node);
        if (coll.isFinished()) {
          coordination.remove(promise);
        }
      }
    }
    if (action.isPresent()) {
      // TODO: run in a context
      CompleteInfo info = action.get();
      info.action().failure(promise, activeNodes);
    }
  }

  public Set<ONodeId> getActiveNodes() {
    return this.publicNodes;
  }
}
