package com.orientechnologies.orient.distributed.context;

import com.orientechnologies.orient.core.transaction.OGroupId;
import com.orientechnologies.orient.core.transaction.ONodeId;
import com.orientechnologies.orient.core.transaction.OTransactionId;
import com.orientechnologies.orient.core.transaction.OTransactionIdPromise;
import com.orientechnologies.orient.distributed.context.coordination.OCoordinatedDistributedOps;
import com.orientechnologies.orient.distributed.context.coordination.OCoordinatedDistributedOpsImpl;
import com.orientechnologies.orient.distributed.context.coordination.OOperationStart;
import com.orientechnologies.orient.distributed.context.coordination.action.OCompleteAction;
import com.orientechnologies.orient.distributed.context.coordination.dbs.ODatabaseStateChangeListener;
import com.orientechnologies.orient.distributed.context.coordination.dbs.ODatabasesTopology;
import com.orientechnologies.orient.distributed.context.coordination.log.ODistributedMessageLog;
import com.orientechnologies.orient.distributed.context.coordination.log.ODistributedMessageLogMemory;
import com.orientechnologies.orient.distributed.context.coordination.message.ODistributedMessage;
import com.orientechnologies.orient.distributed.context.coordination.message.state.ONodeStateNetwork;
import com.orientechnologies.orient.distributed.context.coordination.result.OAcceptResult;
import com.orientechnologies.orient.distributed.context.coordination.topology.ODiscoverAction;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

public class ONodeState {

  private final ODistributedMessageLog log;
  private final OCoordinatedDistributedOps coordinated;
  private final ONodeId nodeId;
  private final OAppliedState state;
  private final OStateStore store;

  public ONodeState(
      ONodeId current,
      OGroupId groupId,
      int minimumQuorum,
      OStateStore store,
      ODatabaseStateChangeListener listener) {
    this.log = new ODistributedMessageLogMemory();
    this.store = store;
    this.coordinated =
        new OCoordinatedDistributedOpsImpl(
            current, groupId, minimumQuorum, listener, this.store::save);
    this.nodeId = current;
    this.state = new OAppliedState(3, (txId) -> this.coordinated.isApplied(txId));
  }

  public ODiscoverAction initFromStore() {
    ONodeStateStore load = store.load();
    coordinated.load(load);
    return coordinated.discoverNode(nodeId);
  }

  public Optional<OOperationStart> start(OCompleteAction action) {
    return this.coordinated.start(action);
  }

  public void success(ONodeId node, OTransactionIdPromise promise) {
    this.coordinated.nodeSuccess(node, promise);
  }

  public void failure(ONodeId node, OTransactionIdPromise promise, OAcceptResult acceptResult) {
    this.coordinated.nodeFailure(node, promise, acceptResult);
  }

  public Optional<OAcceptResult> receive(ODistributedMessage message) {
    var result = this.coordinated.receive(message);
    if (result.isEmpty()) {
      this.log.log(message);
    }
    return result;
  }

  public Optional<ODistributedMessage> receiveFailure(OTransactionIdPromise promise) {
    return this.coordinated.consensusFailure(promise);
  }

  public Optional<ODistributedMessage> receiveSuccess(OTransactionIdPromise promise) {
    return this.coordinated.consensusSuccess(promise);
  }

  public void complete(OTransactionIdPromise promise) {
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

  public ONodeStateNetwork getNetworkState() {
    return this.coordinated.getNetworkState();
  }

  public ODatabasesTopology getDatabaseTopology() {
    return coordinated.getDatabaseTopology();
  }

  public Set<ONodeId> getNetworkMembers() {
    return coordinated.getNetworkMembers();
  }

  public OCoordinatedDistributedOps getOps() {
    return coordinated;
  }

  public void cancelPromise(OTransactionIdPromise promise) {
    coordinated.cancelPromise(promise);
  }
}
