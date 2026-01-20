package com.orientechnologies.orient.distriubted.context;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.log.OLogger;
import com.orientechnologies.orient.core.transaction.ODatabaseId;
import com.orientechnologies.orient.core.transaction.OGroupId;
import com.orientechnologies.orient.core.transaction.ONodeId;
import com.orientechnologies.orient.core.transaction.OTransactionIdPromise;
import com.orientechnologies.orient.core.tx.OTransactionSequenceStatus;
import com.orientechnologies.orient.distributed.context.ONetworkTopologyStore;
import com.orientechnologies.orient.distributed.context.ONodeStateStore;
import com.orientechnologies.orient.distributed.context.ONodeStateUpdated;
import com.orientechnologies.orient.distributed.context.coordination.OCoordinatedDistributedOps;
import com.orientechnologies.orient.distributed.context.coordination.OCoordinatedDistributedOpsImpl;
import com.orientechnologies.orient.distributed.context.coordination.OResponseCollector;
import com.orientechnologies.orient.distributed.context.coordination.OResponseCollectorImpl;
import com.orientechnologies.orient.distributed.context.coordination.OResponseCollectorMerge;
import com.orientechnologies.orient.distributed.context.coordination.action.OCompleteAction;
import com.orientechnologies.orient.distributed.context.coordination.dbs.ODatabaseState;
import com.orientechnologies.orient.distributed.context.coordination.dbs.ODatabaseStateChangeListener;
import com.orientechnologies.orient.distributed.context.coordination.message.state.ONodeStateNetwork;
import com.orientechnologies.orient.distributed.context.coordination.message.state.OTopologyStateNetwork;
import com.orientechnologies.orient.distributed.context.coordination.result.OAcceptResult;
import com.orientechnologies.orient.distributed.context.coordination.result.OAlreadyPromised;
import com.orientechnologies.orient.distributed.context.coordination.topology.ODiscoverAction;
import com.orientechnologies.orient.distributed.context.coordination.topology.OTopologyState;
import java.util.Collections;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import org.junit.Test;

public class OCoordinatedDistributedOpsMergeTest
    implements ODatabaseStateChangeListener, ONodeStateUpdated {
  private static final OLogger logger =
      OLogManager.instance().logger(OCoordinatedDistributedOpsTest.class);

  @Override
  public void onStateChange(ODatabaseId dbId, ONodeId nodeId, ODatabaseState state) {
    logger.debug("change state of  node %s on database %s to %s", nodeId, dbId, state);
  }

  @Override
  public void update(ONodeStateStore newState) {
    logger.debug("new node state stored");
  }

  private class TestAction implements OCompleteAction {
    private boolean success = false;
    private boolean failure = false;
    private boolean complete = false;
    private Optional<OAcceptResult> result;
    private ONodeId toMerge;

    public TestAction(ONodeId toMerge) {
      this.toMerge = toMerge;
    }

    @Override
    public void success(OTransactionIdPromise promise, Set<ONodeId> expected) {
      success = true;
    }

    @Override
    public void failure(
        OTransactionIdPromise promise, Set<ONodeId> expected, Optional<OAcceptResult> result) {
      failure = true;
      this.result = result;
    }

    @Override
    public void complete(
        OTransactionIdPromise promise, Set<ONodeId> nodes, Optional<OAcceptResult> result) {
      complete = true;
      this.result = result;
    }

    @Override
    public OResponseCollector newResponseCollector(
        OTransactionIdPromise promise, int quorum, Set<ONodeId> nodes) {
      if (toMerge == null) {
        return new OResponseCollectorImpl(this, promise, quorum, nodes);
      } else {
        return new OResponseCollectorMerge(this, promise, quorum, nodes, toMerge);
      }
    }
  }

  private ONodeId newRandomNodeId() {
    return new ONodeId(UUID.randomUUID().toString());
  }

  private OGroupId newRandomGroupId() {
    return new OGroupId(UUID.randomUUID().toString());
  }

  private ONetworkTopologyStore bootStoreState(OGroupId groupId) {
    return new ONetworkTopologyStore(groupId, OTopologyState.BOOT, new HashSet<>(), 0, 0);
  }

  private ONodeStateNetwork networkState(OTopologyStateNetwork topology) {
    return new ONodeStateNetwork(
        topology, Collections.emptyList(), new OTransactionSequenceStatus(new long[] {}));
  }

  private ONodeStateNetwork bootNetworkState(OGroupId groupId) {
    return networkState(OTopologyStateNetwork.boot(groupId));
  }

  private OCoordinatedDistributedOps quorum1Env(ONodeId nodeId, OGroupId groupId) {
    OCoordinatedDistributedOps ops =
        new OCoordinatedDistributedOpsImpl(nodeId, groupId, 1, this, this);
    ODiscoverAction action = ops.nodeJoinStart(nodeId, bootNetworkState(groupId), false);
    assertTrue(action instanceof ODiscoverAction.OEstablishAction);
    Optional<OTransactionIdPromise> seq = ops.startEstablish(Set.of(nodeId), new TestAction(null));
    ops.consensusSuccess(seq.get());
    ops.validateEstablish(groupId, Set.of(nodeId));
    ops.establish(groupId, Set.of(nodeId));
    return ops;
  }

  @Test
  public void baseSuccess() {
    ONodeId nodeId1 = newRandomNodeId();
    OGroupId groupId = newRandomGroupId();

    OCoordinatedDistributedOps ops = quorum1Env(nodeId1, groupId);

    ONodeId nodeId2 = newRandomNodeId();

    TestAction action = new TestAction(nodeId2);
    var res = ops.start(action);
    var promise = res.get().promise();

    ops.nodeSuccess(nodeId1, promise);
    assertFalse(action.success);
    assertFalse(action.failure);

    ops.nodeSuccess(nodeId2, promise);

    assertTrue(action.success);
    assertFalse(action.failure);
  }

  @Test
  public void baseSuccessComplete() {
    ONodeId nodeId1 = newRandomNodeId();
    OGroupId groupId = newRandomGroupId();

    OCoordinatedDistributedOps ops = quorum1Env(nodeId1, groupId);

    ONodeId nodeId2 = newRandomNodeId();

    TestAction action = new TestAction(nodeId2);
    var res = ops.start(action);
    var promise = res.get().promise();

    ops.nodeSuccess(nodeId1, promise);
    assertFalse(action.success);
    assertFalse(action.failure);

    ops.nodeSuccess(nodeId2, promise);

    assertTrue(action.success);
    assertFalse(action.failure);
    ops.completeExecution(promise);

    assertTrue(action.success);
    assertFalse(action.failure);
    assertTrue(action.complete);
  }

  @Test
  public void baseFail() {
    ONodeId nodeId1 = newRandomNodeId();
    OGroupId groupId = newRandomGroupId();

    OCoordinatedDistributedOps ops = quorum1Env(nodeId1, groupId);

    ONodeId nodeId2 = newRandomNodeId();

    TestAction action = new TestAction(nodeId2);
    var res = ops.start(action);
    var promise = res.get().promise();

    ops.nodeSuccess(nodeId1, promise);
    assertFalse(action.success);
    assertFalse(action.failure);
    ops.nodeFailure(nodeId2, promise, new OAlreadyPromised());

    assertFalse(action.success);
    assertTrue(action.failure);
  }

  @Test
  public void mergePromiseTest() {
    ONodeId nodeId1 = newRandomNodeId();
    ONodeId nodeId2 = newRandomNodeId();
    OGroupId groupId = newRandomGroupId();

    OCoordinatedDistributedOps ops1 = quorum1Env(nodeId1, groupId);

    TestAction action = new TestAction(nodeId2);
    var res = ops1.start(action);
    var promise = res.get().promise();
    ops1.nodeSuccess(nodeId1, promise);

    OCoordinatedDistributedOps ops2 = quorum1Env(nodeId1, groupId);
    assertTrue(ops2.validateMerge(groupId, nodeId2));
    ops1.confirmMerge(nodeId2, promise, true);
    assertTrue(action.success);
    assertFalse(action.failure);
  }

  @Test
  public void mergeDoublePromiseTest() {
    ONodeId nodeId1 = newRandomNodeId();
    ONodeId nodeId2 = newRandomNodeId();
    ONodeId nodeId3 = newRandomNodeId();
    OGroupId groupId = newRandomGroupId();

    OCoordinatedDistributedOps ops1 = quorum1Env(nodeId1, groupId);

    TestAction action = new TestAction(nodeId2);
    var res = ops1.start(action);
    var promise = res.get().promise();
    ops1.nodeSuccess(nodeId1, promise);

    OCoordinatedDistributedOps ops3 = quorum1Env(nodeId1, groupId);

    TestAction action1 = new TestAction(nodeId2);
    var res1 = ops3.start(action1);
    var promise1 = res1.get().promise();
    ops3.nodeSuccess(nodeId3, promise1);

    OCoordinatedDistributedOps ops2 = quorum1Env(nodeId1, groupId);
    assertTrue(ops2.validateMerge(groupId, nodeId2));
    assertFalse(ops2.validateMerge(groupId, nodeId3));

    ops1.confirmMerge(nodeId2, promise, true);
    assertTrue(action.success);
    assertFalse(action.failure);

    ops3.confirmMerge(nodeId2, promise1, false);
    assertFalse(action1.success);
    assertTrue(action1.failure);
  }
}
