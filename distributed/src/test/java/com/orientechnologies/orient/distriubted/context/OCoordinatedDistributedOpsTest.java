package com.orientechnologies.orient.distriubted.context;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.orientechnologies.orient.core.transaction.OGroupId;
import com.orientechnologies.orient.core.transaction.ONodeId;
import com.orientechnologies.orient.core.transaction.OTransactionId;
import com.orientechnologies.orient.core.transaction.OTransactionIdPromise;
import com.orientechnologies.orient.distributed.context.OCompleteAction;
import com.orientechnologies.orient.distributed.context.OCoordinatedDistributedOps;
import com.orientechnologies.orient.distributed.context.OCoordinatedDistributedOpsImpl;
import com.orientechnologies.orient.distributed.context.ONodeStateStore;
import com.orientechnologies.orient.distributed.context.OOperationStart;
import com.orientechnologies.orient.distributed.context.coordination.message.ONodeStateNetwork;
import com.orientechnologies.orient.distributed.context.coordination.result.OAcceptResult;
import com.orientechnologies.orient.distributed.context.coordination.result.OInvalidSequential;
import com.orientechnologies.orient.distributed.context.coordination.result.OQuorumNotReached;
import com.orientechnologies.orient.distributed.context.topology.ODiscoverAction;
import com.orientechnologies.orient.distributed.context.topology.OTopologyState;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import org.junit.Test;

public class OCoordinatedDistributedOpsTest {

  private class TestAction implements OCompleteAction {
    private boolean success = false;
    private boolean failure = false;

    @Override
    public void success(OTransactionIdPromise promise, Set<ONodeId> expected) {
      success = true;
    }

    @Override
    public void failure(OTransactionIdPromise promise, Set<ONodeId> expected) {
      failure = true;
    }
  }

  private ONodeId newRandomNodeId() {
    return new ONodeId(UUID.randomUUID().toString());
  }

  private OGroupId newRandomGroupId() {
    return new OGroupId(UUID.randomUUID().toString());
  }

  @Test
  public void baseSuccess() {
    TestAction action = new TestAction();
    ONodeId nodeId = newRandomNodeId();
    OCoordinatedDistributedOps ops = new OCoordinatedDistributedOpsImpl(nodeId, 2);
    ops.registerNode(nodeId, 0);
    ONodeId nodeIdtwo = newRandomNodeId();
    ops.registerNode(nodeIdtwo, 0);

    OTransactionId txId = new OTransactionId(0, 1);
    OTransactionIdPromise promise = new OTransactionIdPromise(nodeId, txId);
    ops.start(promise, action);
    ops.success(nodeId, promise);
    ops.success(nodeIdtwo, promise);

    assertTrue(action.success);
    assertFalse(action.failure);
  }

  @Test
  public void baseFail() {
    TestAction action = new TestAction();
    ONodeId nodeId = newRandomNodeId();
    OCoordinatedDistributedOps ops = new OCoordinatedDistributedOpsImpl(nodeId, 2);
    ops.registerNode(nodeId, 0);
    ONodeId nodeIdtwo = newRandomNodeId();
    ops.registerNode(nodeIdtwo, 0);

    OTransactionId txId = new OTransactionId(0, 1);
    OTransactionIdPromise promise = new OTransactionIdPromise(nodeId, txId);
    ops.start(promise, action);
    ops.success(nodeId, promise);
    ops.failure(nodeIdtwo, promise, new OInvalidSequential());

    assertFalse(action.success);
    assertTrue(action.failure);
  }

  @Test
  public void baseSuccessMoreNodes() {
    TestAction action = new TestAction();
    ONodeId nodeId = newRandomNodeId();
    OCoordinatedDistributedOps ops = new OCoordinatedDistributedOpsImpl(nodeId, 2);
    ops.registerNode(nodeId, 0);
    ONodeId nodeIdtwo = newRandomNodeId();
    ops.registerNode(nodeIdtwo, 0);
    ONodeId nodeIdthree = newRandomNodeId();
    ops.registerNode(nodeIdthree, 0);
    ONodeId nodeIdFour = newRandomNodeId();
    ops.registerNode(nodeIdFour, 0);

    OTransactionId txId = new OTransactionId(0, 1);
    OTransactionIdPromise promise = new OTransactionIdPromise(nodeId, txId);
    ops.start(promise, action);
    ops.success(nodeId, promise);
    ops.success(nodeIdtwo, promise);
    assertFalse(action.success);
    ops.success(nodeIdthree, promise);
    assertTrue(action.success);
    ops.success(nodeIdFour, promise);

    assertTrue(action.success);
    assertFalse(action.failure);
  }

  @Test
  public void baseSuccessMoreNodesUregistered() {
    TestAction action = new TestAction();
    ONodeId nodeId = newRandomNodeId();
    OCoordinatedDistributedOps ops = new OCoordinatedDistributedOpsImpl(nodeId, 2);
    ops.registerNode(nodeId, 0);
    ONodeId nodeIdtwo = newRandomNodeId();
    ops.registerNode(nodeIdtwo, 0);
    ONodeId nodeIdthree = newRandomNodeId();
    ops.registerNode(nodeIdthree, 0);
    ONodeId nodeIdFour = newRandomNodeId();
    ops.registerNode(nodeIdFour, 0);

    ops.unregisterNode(nodeIdthree, 0);
    ops.unregisterNode(nodeIdFour, 0);

    OTransactionId txId = new OTransactionId(0, 1);
    OTransactionIdPromise promise = new OTransactionIdPromise(nodeId, txId);
    ops.start(promise, action);
    ops.success(nodeId, promise);
    ops.success(nodeIdtwo, promise);
    assertTrue(action.success);
    assertFalse(action.failure);
  }

  @Test
  public void baseFailureeNodesUregistered() throws InterruptedException, ExecutionException {
    TestAction action = new TestAction();
    ONodeId nodeId = newRandomNodeId();
    OCoordinatedDistributedOps ops = new OCoordinatedDistributedOpsImpl(nodeId, 2);
    ops.registerNode(nodeId, 0);
    ONodeId nodeIdtwo = newRandomNodeId();
    ops.registerNode(nodeIdtwo, 0);

    OTransactionId txId = new OTransactionId(0, 1);
    OTransactionIdPromise promise = new OTransactionIdPromise(nodeId, txId);
    OOperationStart start = ops.start(promise, action);
    ops.success(nodeId, promise);
    ops.unregisterNode(nodeIdtwo, 0);
    assertTrue(action.failure);
    assertFalse(action.success);
    assertTrue(start.result().get().get() instanceof OQuorumNotReached);
  }

  @Test
  public void baseFailureeNodesUregisteredAndFailed()
      throws InterruptedException, ExecutionException {
    TestAction action = new TestAction();
    ONodeId nodeId = newRandomNodeId();
    OCoordinatedDistributedOps ops = new OCoordinatedDistributedOpsImpl(nodeId, 2);
    ops.registerNode(nodeId, 0);
    ONodeId nodeIdtwo = newRandomNodeId();
    ops.registerNode(nodeIdtwo, 0);
    ONodeId nodeIdthree = newRandomNodeId();
    ops.registerNode(nodeIdthree, 0);
    ONodeId nodeIdFour = newRandomNodeId();
    ops.registerNode(nodeIdFour, 0);

    OTransactionId txId = new OTransactionId(0, 1);
    OTransactionIdPromise promise = new OTransactionIdPromise(nodeId, txId);
    OOperationStart start = ops.start(promise, action);
    ops.success(nodeId, promise);
    ops.success(nodeIdtwo, promise);
    ops.unregisterNode(nodeIdthree, 0);
    ops.failure(nodeIdFour, promise, new OInvalidSequential());
    assertTrue(action.failure);
    assertFalse(action.success);
    assertTrue(start.result().get().get() instanceof OQuorumNotReached);
  }

  @Test
  public void baseNodeDiscoverEmpty() {
    ONodeId nodeId = newRandomNodeId();
    OCoordinatedDistributedOps ops = new OCoordinatedDistributedOpsImpl(nodeId, 2);
    ODiscoverAction action = ops.nodeJoinStart(nodeId, ONodeStateNetwork.boot());
    assertTrue(action instanceof ODiscoverAction.ONoneAction);
    assertTrue(ops.getMembers().isEmpty());
  }

  @Test
  public void baseNodeDiscoverReachQuorum() {
    ONodeId nodeId = newRandomNodeId();
    OCoordinatedDistributedOps ops = new OCoordinatedDistributedOpsImpl(nodeId, 2);
    ODiscoverAction action = ops.nodeJoinStart(nodeId, ONodeStateNetwork.boot());
    assertTrue(action instanceof ODiscoverAction.ONoneAction);

    ONodeId nodeId1 = newRandomNodeId();
    action = ops.nodeJoinStart(nodeId1, ONodeStateNetwork.boot());

    assertTrue(action instanceof ODiscoverAction.OEstablishAction);
    ops.enstablish(
        ((ODiscoverAction.OEstablishAction) action).groupId(),
        ((ODiscoverAction.OEstablishAction) action).candidates());

    assertEquals(ops.getMembers().size(), 2);
  }

  @Test
  public void nodeDiscoverAddNode() {
    ONodeId nodeId = newRandomNodeId();
    OCoordinatedDistributedOps ops = new OCoordinatedDistributedOpsImpl(nodeId, 2);
    ODiscoverAction action = ops.nodeJoinStart(nodeId, ONodeStateNetwork.boot());

    assertTrue(action instanceof ODiscoverAction.ONoneAction);

    ONodeId nodeId1 = newRandomNodeId();
    action = ops.nodeJoinStart(nodeId1, ONodeStateNetwork.boot());

    assertTrue(action instanceof ODiscoverAction.OEstablishAction);
    ops.enstablish(
        ((ODiscoverAction.OEstablishAction) action).groupId(),
        ((ODiscoverAction.OEstablishAction) action).candidates());

    assertEquals(ops.getMembers().size(), 2);
    ONodeId nodeId2 = newRandomNodeId();
    action = ops.nodeJoinStart(nodeId2, ONodeStateNetwork.boot());
    assertTrue(action instanceof ODiscoverAction.OAddNodeAction);
    ops.registerNode(
        ((ODiscoverAction.OAddNodeAction) action).node(),
        ((ODiscoverAction.OAddNodeAction) action).version());
    assertEquals(ops.getMembers().size(), 3);
  }

  @Test
  public void threeNodesJoinCoordinationDefine() {
    ONodeId nodeId1 = newRandomNodeId();
    OCoordinatedDistributedOps node1 = new OCoordinatedDistributedOpsImpl(nodeId1, 2);
    ONodeId nodeId2 = newRandomNodeId();
    OCoordinatedDistributedOps node2 = new OCoordinatedDistributedOpsImpl(nodeId2, 2);
    ONodeId nodeId3 = newRandomNodeId();

    ODiscoverAction action = node1.nodeJoinStart(nodeId1, ONodeStateNetwork.boot());
    assertTrue(action instanceof ODiscoverAction.ONoneAction);

    action = node1.nodeJoinStart(nodeId2, ONodeStateNetwork.boot());
    assertTrue(action instanceof ODiscoverAction.OEstablishAction);
    var candidates = ((ODiscoverAction.OEstablishAction) action).candidates();
    var networkId = ((ODiscoverAction.OEstablishAction) action).groupId();
    Optional<OAcceptResult> result = node2.validateEnstablish(networkId, candidates);
    assertTrue(result.isEmpty());
    assertTrue(result.isEmpty());

    node1.enstablish(networkId, candidates);
    node2.enstablish(networkId, candidates);
    assertEquals(node1.getMembers().size(), 2);
    assertEquals(node2.getMembers().size(), 2);

    action = node1.nodeJoinStart(nodeId3, ONodeStateNetwork.boot());
    assertTrue(action instanceof ODiscoverAction.OAddNodeAction);
    var addNode = ((ODiscoverAction.OAddNodeAction) action).node();
    var addVersion = ((ODiscoverAction.OAddNodeAction) action).version();
    assertTrue(addVersion > 0);
    var res = node1.promiseRegister(addNode, addVersion);
    assertTrue(res);
    res = node2.promiseRegister(addNode, addVersion);
    assertTrue(res);
    node1.registerNode(addNode, addVersion);
    node2.registerNode(addNode, addVersion);

    assertEquals(node1.getMembers().size(), 3);
    assertEquals(node2.getMembers().size(), 3);
  }

  @Test
  public void threeNodesJoinCoordinationFail() {
    ONodeId nodeId1 = newRandomNodeId();
    OCoordinatedDistributedOps node1 = new OCoordinatedDistributedOpsImpl(nodeId1, 2);
    ONodeId nodeId2 = newRandomNodeId();
    OCoordinatedDistributedOps node2 = new OCoordinatedDistributedOpsImpl(nodeId2, 2);
    ONodeId nodeId3 = newRandomNodeId();

    ODiscoverAction action = node1.nodeJoinStart(nodeId1, ONodeStateNetwork.boot());

    assertTrue(action instanceof ODiscoverAction.ONoneAction);

    action = node1.nodeJoinStart(nodeId2, ONodeStateNetwork.boot());
    assertTrue(action instanceof ODiscoverAction.OEstablishAction);
    var candidates = ((ODiscoverAction.OEstablishAction) action).candidates();
    var networkId = ((ODiscoverAction.OEstablishAction) action).groupId();
    Optional<OAcceptResult> result = node2.validateEnstablish(networkId, candidates);
    assertTrue(result.isEmpty());
    assertTrue(result.isEmpty());

    node1.enstablish(networkId, candidates);
    node2.enstablish(networkId, candidates);
    assertEquals(node1.getMembers().size(), 2);
    assertEquals(node2.getMembers().size(), 2);

    action = node1.nodeJoinStart(nodeId3, ONodeStateNetwork.boot());
    assertTrue(action instanceof ODiscoverAction.OAddNodeAction);

    var addNode = ((ODiscoverAction.OAddNodeAction) action).node();
    var addVersion = ((ODiscoverAction.OAddNodeAction) action).version() - 1;

    var res = node1.promiseRegister(addNode, addVersion);
    assertFalse(res);
    res = node2.promiseRegister(addNode, addVersion);
    assertFalse(res);

    assertEquals(node1.getMembers().size(), 2);
    assertEquals(node2.getMembers().size(), 2);
  }

  @Test
  public void nodesJoinAlreadyDefinedMoreRecent() {
    ONodeId nodeId1 = newRandomNodeId();
    OCoordinatedDistributedOps node1 = new OCoordinatedDistributedOpsImpl(nodeId1, 2);
    ONodeId nodeId2 = newRandomNodeId();
    ONodeId nodeId3 = newRandomNodeId();
    var gid = newRandomGroupId();
    node1.load(
        new ONodeStateStore(
            Optional.of(gid), OTopologyState.ESTABLISHED, Set.of(nodeId1, nodeId2), 2));
    node1.discoverNode(nodeId1);
    assertEquals(node1.getMembers().size(), 2);

    ODiscoverAction action =
        node1.nodeJoinStart(
            nodeId2,
            new ONodeStateNetwork(
                Optional.of(gid),
                OTopologyState.ESTABLISHED,
                Set.of(nodeId1, nodeId2, nodeId3),
                3));
    assertTrue(action instanceof ODiscoverAction.ONoneAction);

    assertEquals(node1.getMembers().size(), 3);
  }

  @Test
  public void nodesJoinAlreadyDefinedOutdated() {
    ONodeId nodeId1 = newRandomNodeId();
    OCoordinatedDistributedOps node1 = new OCoordinatedDistributedOpsImpl(nodeId1, 2);
    ONodeId nodeId2 = newRandomNodeId();
    ONodeId nodeId3 = newRandomNodeId();
    var gid = newRandomGroupId();
    node1.load(
        new ONodeStateStore(
            Optional.of(gid), OTopologyState.ESTABLISHED, Set.of(nodeId1, nodeId2, nodeId3), 3));
    node1.discoverNode(nodeId1);
    assertEquals(node1.getMembers().size(), 3);

    ODiscoverAction action =
        node1.nodeJoinStart(
            nodeId2,
            new ONodeStateNetwork(
                Optional.of(gid), OTopologyState.ESTABLISHED, Set.of(nodeId1, nodeId2), 2));
    assertTrue(action instanceof ODiscoverAction.ONoneAction);

    assertEquals(node1.getMembers().size(), 3);
  }

  @Test
  public void nodesJoinAlreadyDefinedNewNode() {
    ONodeId nodeId1 = newRandomNodeId();
    OCoordinatedDistributedOps node1 = new OCoordinatedDistributedOpsImpl(nodeId1, 2);
    ONodeId nodeId2 = newRandomNodeId();
    ONodeId nodeId3 = newRandomNodeId();
    var gid = newRandomGroupId();
    node1.load(
        new ONodeStateStore(
            Optional.of(gid), OTopologyState.ESTABLISHED, Set.of(nodeId1, nodeId2), 3));
    node1.discoverNode(nodeId1);
    assertEquals(node1.getMembers().size(), 2);

    ODiscoverAction action = node1.nodeJoinStart(nodeId3, ONodeStateNetwork.boot());
    assertTrue(action instanceof ODiscoverAction.OAddNodeAction);

    assertEquals(node1.getMembers().size(), 2);
  }

  @Test
  public void nodesNotDefineInoreTopology() {
    ONodeId nodeId1 = newRandomNodeId();
    OCoordinatedDistributedOps node1 = new OCoordinatedDistributedOpsImpl(nodeId1, 2);
    ONodeId nodeId2 = newRandomNodeId();
    ONodeId nodeId3 = newRandomNodeId();
    var gid = newRandomGroupId();
    node1.load(new ONodeStateStore(Optional.empty(), OTopologyState.BOOT, new HashSet<>(), 0));
    node1.discoverNode(nodeId1);
    assertEquals(node1.getMembers().size(), 0);

    ODiscoverAction action =
        node1.nodeJoinStart(
            nodeId2,
            new ONodeStateNetwork(
                Optional.of(gid), OTopologyState.ESTABLISHED, Set.of(nodeId2, nodeId3), 2));
    assertTrue(action instanceof ODiscoverAction.ONoneAction);

    assertEquals(node1.getMembers().size(), 0);
  }

  @Test
  public void nodesBootGetDefinedTopology() {
    ONodeId nodeId1 = newRandomNodeId();
    OCoordinatedDistributedOps node1 = new OCoordinatedDistributedOpsImpl(nodeId1, 2);
    ONodeId nodeId2 = newRandomNodeId();
    var gid = newRandomGroupId();
    node1.load(new ONodeStateStore(Optional.empty(), OTopologyState.BOOT, new HashSet<>(), 0));
    node1.discoverNode(nodeId1);
    assertEquals(node1.getMembers().size(), 0);

    ODiscoverAction action =
        node1.nodeJoinStart(
            nodeId2,
            new ONodeStateNetwork(
                Optional.of(gid), OTopologyState.ESTABLISHED, Set.of(nodeId1, nodeId2), 2));
    assertTrue(action instanceof ODiscoverAction.ONoneAction);

    assertEquals(node1.getMembers().size(), 2);
  }
}
