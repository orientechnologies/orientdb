package com.orientechnologies.orient.distriubted.context;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.orientechnologies.orient.core.transaction.OGroupId;
import com.orientechnologies.orient.core.transaction.ONodeId;
import com.orientechnologies.orient.core.transaction.OTransactionId;
import com.orientechnologies.orient.core.transaction.OTransactionIdPromise;
import com.orientechnologies.orient.distributed.context.OCompleteAction;
import com.orientechnologies.orient.distributed.context.OCoordinatedDistributedOps;
import com.orientechnologies.orient.distributed.context.OCoordinatedDistributedOpsImpl;
import com.orientechnologies.orient.distributed.context.ONodeStateStore;
import com.orientechnologies.orient.distributed.context.OOperationStart;
import com.orientechnologies.orient.distributed.context.coordination.message.OTopologyStateNetwork;
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
    private boolean complete = false;
    private Optional<OAcceptResult> result;

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
    OGroupId groupId = newRandomGroupId();
    OCoordinatedDistributedOps ops = new OCoordinatedDistributedOpsImpl(nodeId, groupId, 2);
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
  public void baseSuccessComplete() {
    TestAction action = new TestAction();
    ONodeId nodeId = newRandomNodeId();
    OGroupId groupId = newRandomGroupId();
    OCoordinatedDistributedOps ops = new OCoordinatedDistributedOpsImpl(nodeId, groupId, 2);
    ops.registerNode(nodeId, 0);
    ONodeId nodeIdtwo = newRandomNodeId();
    ops.registerNode(nodeIdtwo, 0);

    OTransactionId txId = new OTransactionId(0, 1);
    OTransactionIdPromise promise = new OTransactionIdPromise(nodeId, txId);
    ops.start(promise, action);
    ops.success(nodeId, promise);
    ops.success(nodeIdtwo, promise);
    ops.completeExecution(promise);

    assertTrue(action.success);
    assertFalse(action.failure);
    assertTrue(action.complete);
  }

  @Test
  public void baseFail() {
    TestAction action = new TestAction();
    ONodeId nodeId = newRandomNodeId();
    OGroupId groupId = newRandomGroupId();
    OCoordinatedDistributedOps ops = new OCoordinatedDistributedOpsImpl(nodeId, groupId, 2);
    ONodeId nodeIdtwo = newRandomNodeId();
    ops.enstablish(groupId, Set.of(nodeId, nodeIdtwo));

    OTransactionId txId = new OTransactionId(0, 1);
    OTransactionIdPromise promise = new OTransactionIdPromise(nodeId, txId);
    ops.start(promise, action);
    ops.success(nodeId, promise);
    ops.failure(nodeIdtwo, promise, new OInvalidSequential(0, 0));

    assertFalse(action.success);
    assertTrue(action.failure);
  }

  @Test
  public void baseSuccessMoreNodes() {
    TestAction action = new TestAction();
    ONodeId nodeId = newRandomNodeId();
    OGroupId groupId = new OGroupId("group");
    OCoordinatedDistributedOps ops = new OCoordinatedDistributedOpsImpl(nodeId, groupId, 2);
    ONodeId nodeIdtwo = newRandomNodeId();
    ONodeId nodeIdthree = newRandomNodeId();
    ONodeId nodeIdFour = newRandomNodeId();
    ops.enstablish(groupId, Set.of(nodeId, nodeIdtwo, nodeIdthree, nodeIdFour));

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
    OGroupId groupId = newRandomGroupId();
    OCoordinatedDistributedOps ops = new OCoordinatedDistributedOpsImpl(nodeId, groupId, 2);
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
    OGroupId groupId = new OGroupId("group");
    OCoordinatedDistributedOps ops = new OCoordinatedDistributedOpsImpl(nodeId, groupId, 2);
    ONodeId nodeIdtwo = newRandomNodeId();
    Set<ONodeId> nodes = Set.of(nodeId, nodeIdtwo);
    ops.enstablish(groupId, nodes);

    OTransactionId txId = new OTransactionId(0, 1);
    OTransactionIdPromise promise = new OTransactionIdPromise(nodeId, txId);
    OOperationStart start = ops.start(promise, action);
    assertTrue(start.nodes().containsAll(nodes));
    ops.success(nodeId, promise);
    ops.unregisterNode(nodeIdtwo, 0);
    assertTrue(action.failure);
    assertFalse(action.success);
    assertTrue(action.result.get() instanceof OQuorumNotReached);
  }

  @Test
  public void baseFailureeNodesUregisteredAndFailed()
      throws InterruptedException, ExecutionException {
    TestAction action = new TestAction();
    ONodeId nodeId = newRandomNodeId();
    OGroupId groupId = new OGroupId("group");
    OCoordinatedDistributedOps ops = new OCoordinatedDistributedOpsImpl(nodeId, groupId, 2);
    ONodeId nodeIdtwo = newRandomNodeId();
    ONodeId nodeIdthree = newRandomNodeId();
    ONodeId nodeIdFour = newRandomNodeId();
    Set<ONodeId> nodes = Set.of(nodeId, nodeIdtwo, nodeIdthree, nodeIdFour);
    ops.enstablish(groupId, nodes);

    OTransactionId txId = new OTransactionId(0, 1);
    OTransactionIdPromise promise = new OTransactionIdPromise(nodeId, txId);
    OOperationStart start = ops.start(promise, action);
    assertTrue(start.nodes().containsAll(nodes));
    ops.success(nodeId, promise);
    ops.success(nodeIdtwo, promise);
    ops.unregisterNode(nodeIdthree, 0);
    ops.failure(nodeIdFour, promise, new OInvalidSequential(0, 0));
    assertTrue(action.failure);
    assertFalse(action.success);
    assertTrue(action.result.get() instanceof OQuorumNotReached);
  }

  @Test
  public void baseNodeDiscoverEmpty() {
    ONodeId nodeId = newRandomNodeId();
    OGroupId groupId = newRandomGroupId();
    OCoordinatedDistributedOps ops = new OCoordinatedDistributedOpsImpl(nodeId, groupId, 2);
    ODiscoverAction action = ops.nodeJoinStart(nodeId, OTopologyStateNetwork.boot(groupId));
    assertTrue(action instanceof ODiscoverAction.ONoneAction);
    assertTrue(ops.getMembers().isEmpty());
  }

  @Test
  public void baseNodeDiscoverReachQuorum() {
    ONodeId nodeId = newRandomNodeId();
    OGroupId groupId = newRandomGroupId();
    OCoordinatedDistributedOps ops = new OCoordinatedDistributedOpsImpl(nodeId, groupId, 2);
    ODiscoverAction action = ops.nodeJoinStart(nodeId, OTopologyStateNetwork.boot(groupId));
    assertTrue(action instanceof ODiscoverAction.ONoneAction);

    ONodeId nodeId1 = newRandomNodeId();
    action = ops.nodeJoinStart(nodeId1, OTopologyStateNetwork.boot(groupId));

    assertTrue(action instanceof ODiscoverAction.OEstablishAction);
    ops.enstablish(
        ((ODiscoverAction.OEstablishAction) action).groupId(),
        ((ODiscoverAction.OEstablishAction) action).candidates());

    assertEquals(ops.getMembers().size(), 2);
    assertEquals(ops.getNetworkState().getQuorum(), 2);
  }

  @Test
  public void nodeDiscoverAddNode() {
    ONodeId nodeId = newRandomNodeId();
    OGroupId groupId = newRandomGroupId();
    OCoordinatedDistributedOps ops = new OCoordinatedDistributedOpsImpl(nodeId, groupId, 2);
    ODiscoverAction action = ops.nodeJoinStart(nodeId, OTopologyStateNetwork.boot(groupId));

    assertTrue(action instanceof ODiscoverAction.ONoneAction);

    ONodeId nodeId1 = newRandomNodeId();
    action = ops.nodeJoinStart(nodeId1, OTopologyStateNetwork.boot(groupId));

    assertTrue(action instanceof ODiscoverAction.OEstablishAction);
    ops.enstablish(
        ((ODiscoverAction.OEstablishAction) action).groupId(),
        ((ODiscoverAction.OEstablishAction) action).candidates());

    assertEquals(ops.getMembers().size(), 2);
    ONodeId nodeId2 = newRandomNodeId();
    action = ops.nodeJoinStart(nodeId2, OTopologyStateNetwork.boot(groupId));
    assertTrue(action instanceof ODiscoverAction.OAddNodeAction);
    ops.registerNode(
        ((ODiscoverAction.OAddNodeAction) action).node(),
        ((ODiscoverAction.OAddNodeAction) action).version());
    assertEquals(ops.getMembers().size(), 3);
  }

  @Test
  public void threeNodesJoinCoordinationDefine() {
    OGroupId groupId = newRandomGroupId();
    ONodeId nodeId1 = newRandomNodeId();
    OCoordinatedDistributedOps node1 = new OCoordinatedDistributedOpsImpl(nodeId1, groupId, 2);
    ONodeId nodeId2 = newRandomNodeId();
    OCoordinatedDistributedOps node2 = new OCoordinatedDistributedOpsImpl(nodeId2, groupId, 2);
    ONodeId nodeId3 = newRandomNodeId();

    ODiscoverAction action = node1.nodeJoinStart(nodeId1, OTopologyStateNetwork.boot(groupId));
    assertTrue(action instanceof ODiscoverAction.ONoneAction);

    action = node1.nodeJoinStart(nodeId2, OTopologyStateNetwork.boot(groupId));
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

    action = node1.nodeJoinStart(nodeId3, OTopologyStateNetwork.boot(groupId));
    assertTrue(action instanceof ODiscoverAction.OAddNodeAction);
    var addNode = ((ODiscoverAction.OAddNodeAction) action).node();
    var addVersion = ((ODiscoverAction.OAddNodeAction) action).version();
    assertTrue(addVersion > 0);
    var res = node1.promiseRegister(addNode, addVersion);
    assertTrue(res.isEmpty());
    res = node2.promiseRegister(addNode, addVersion);
    assertTrue(res.isEmpty());
    node1.registerNode(addNode, addVersion);
    node2.registerNode(addNode, addVersion);

    assertEquals(node1.getMembers().size(), 3);
    assertEquals(node2.getMembers().size(), 3);
  }

  @Test
  public void threeNodesJoinCoordinationFail() {
    OGroupId groupId = newRandomGroupId();
    ONodeId nodeId1 = newRandomNodeId();
    OCoordinatedDistributedOps node1 = new OCoordinatedDistributedOpsImpl(nodeId1, groupId, 2);
    ONodeId nodeId2 = newRandomNodeId();
    OCoordinatedDistributedOps node2 = new OCoordinatedDistributedOpsImpl(nodeId2, groupId, 2);
    ONodeId nodeId3 = newRandomNodeId();

    ODiscoverAction action = node1.nodeJoinStart(nodeId1, OTopologyStateNetwork.boot(groupId));

    assertTrue(action instanceof ODiscoverAction.ONoneAction);

    action = node1.nodeJoinStart(nodeId2, OTopologyStateNetwork.boot(groupId));
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

    action = node1.nodeJoinStart(nodeId3, OTopologyStateNetwork.boot(groupId));
    assertTrue(action instanceof ODiscoverAction.OAddNodeAction);

    var addNode = ((ODiscoverAction.OAddNodeAction) action).node();
    var addVersion = ((ODiscoverAction.OAddNodeAction) action).version() - 1;

    var res = node1.promiseRegister(addNode, addVersion);
    assertFalse(res.isEmpty());
    res = node2.promiseRegister(addNode, addVersion);
    assertFalse(res.isEmpty());

    assertEquals(node1.getMembers().size(), 2);
    assertEquals(node2.getMembers().size(), 2);
    try {
      node1.getMembers().add(newRandomNodeId());
      fail();
    } catch (UnsupportedOperationException e) {
    }
  }

  @Test
  public void nodesJoinAlreadyDefinedMoreRecent() {
    ONodeId nodeId1 = newRandomNodeId();
    OGroupId groupId = newRandomGroupId();
    OCoordinatedDistributedOps node1 = new OCoordinatedDistributedOpsImpl(nodeId1, groupId, 2);
    ONodeId nodeId2 = newRandomNodeId();
    ONodeId nodeId3 = newRandomNodeId();
    var gid = newRandomGroupId();
    node1.load(
        new ONodeStateStore(gid, OTopologyState.ESTABLISHED, Set.of(nodeId1, nodeId2), 2, 2));
    node1.discoverNode(nodeId1);
    assertEquals(node1.getMembers().size(), 2);

    ODiscoverAction action =
        node1.nodeJoinStart(
            nodeId2,
            new OTopologyStateNetwork(
                gid, OTopologyState.ESTABLISHED, Set.of(nodeId1, nodeId2, nodeId3), 2, 3));
    assertTrue(action instanceof ODiscoverAction.ONoneAction);

    assertEquals(node1.getMembers().size(), 3);
    try {
      node1.getMembers().add(newRandomNodeId());
      fail();
    } catch (UnsupportedOperationException e) {
    }
  }

  @Test
  public void nodesJoinAlreadyDefinedOutdated() {
    ONodeId nodeId1 = newRandomNodeId();
    OGroupId groupId = newRandomGroupId();
    OCoordinatedDistributedOps node1 = new OCoordinatedDistributedOpsImpl(nodeId1, groupId, 2);
    ONodeId nodeId2 = newRandomNodeId();
    ONodeId nodeId3 = newRandomNodeId();
    node1.load(
        new ONodeStateStore(
            groupId, OTopologyState.ESTABLISHED, Set.of(nodeId1, nodeId2, nodeId3), 2, 3));
    node1.discoverNode(nodeId1);
    assertEquals(node1.getMembers().size(), 3);
    assertEquals(node1.getNetworkState().getQuorum(), 2);

    ODiscoverAction action =
        node1.nodeJoinStart(
            nodeId2,
            new OTopologyStateNetwork(
                groupId, OTopologyState.ESTABLISHED, Set.of(nodeId1, nodeId2), 2, 2));
    assertTrue(action instanceof ODiscoverAction.ONotifySelf);

    assertEquals(node1.getMembers().size(), 3);
  }

  @Test
  public void nodesJoinAlreadyDefinedNewNode() {
    ONodeId nodeId1 = newRandomNodeId();
    OGroupId groupId = newRandomGroupId();
    OCoordinatedDistributedOps node1 = new OCoordinatedDistributedOpsImpl(nodeId1, groupId, 2);
    ONodeId nodeId2 = newRandomNodeId();
    ONodeId nodeId3 = newRandomNodeId();
    node1.load(
        new ONodeStateStore(groupId, OTopologyState.ESTABLISHED, Set.of(nodeId1, nodeId2), 2, 3));
    node1.discoverNode(nodeId1);
    assertEquals(node1.getMembers().size(), 2);

    ODiscoverAction action = node1.nodeJoinStart(nodeId3, OTopologyStateNetwork.boot(groupId));
    assertTrue(action instanceof ODiscoverAction.OAddNodeAction);

    assertEquals(node1.getMembers().size(), 2);
  }

  @Test
  public void nodesNotDefineInoreTopology() {
    ONodeId nodeId1 = newRandomNodeId();
    OGroupId groupId = newRandomGroupId();
    OCoordinatedDistributedOps node1 = new OCoordinatedDistributedOpsImpl(nodeId1, groupId, 2);
    ONodeId nodeId2 = newRandomNodeId();
    ONodeId nodeId3 = newRandomNodeId();
    var gid = newRandomGroupId();
    node1.load(new ONodeStateStore(groupId, OTopologyState.BOOT, new HashSet<>(), 0, 0));
    node1.discoverNode(nodeId1);
    assertEquals(node1.getMembers().size(), 0);

    ODiscoverAction action =
        node1.nodeJoinStart(
            nodeId2,
            new OTopologyStateNetwork(
                gid, OTopologyState.ESTABLISHED, Set.of(nodeId2, nodeId3), 2, 2));
    assertTrue(action instanceof ODiscoverAction.ONoneAction);

    assertEquals(node1.getMembers().size(), 0);
  }

  @Test
  public void nodesBootGetDefinedTopology() {
    ONodeId nodeId1 = newRandomNodeId();
    OGroupId groupId = newRandomGroupId();
    OCoordinatedDistributedOps node1 = new OCoordinatedDistributedOpsImpl(nodeId1, groupId, 2);
    ONodeId nodeId2 = newRandomNodeId();
    node1.load(new ONodeStateStore(groupId, OTopologyState.BOOT, new HashSet<>(), 0, 0));
    node1.discoverNode(nodeId1);
    assertEquals(node1.getMembers().size(), 0);

    ODiscoverAction action =
        node1.nodeJoinStart(
            nodeId2,
            new OTopologyStateNetwork(
                groupId, OTopologyState.ESTABLISHED, Set.of(nodeId1, nodeId2), 2, 2));
    assertTrue(action instanceof ODiscoverAction.ONoneAction);

    assertEquals(node1.getMembers().size(), 2);
    try {
      node1.getMembers().add(newRandomNodeId());
      fail();
    } catch (UnsupportedOperationException e) {
    }
  }

  @Test
  public void twoNodesQuorumOneMergeRequest() {
    ONodeId nodeId1 = newRandomNodeId();
    ONodeId nodeId2 = newRandomNodeId();
    OGroupId groupId = newRandomGroupId();

    // First node quorum1 establish
    OCoordinatedDistributedOps ops1 = new OCoordinatedDistributedOpsImpl(nodeId1, groupId, 1);
    ODiscoverAction action1 = ops1.nodeJoinStart(nodeId1, OTopologyStateNetwork.boot(groupId));
    assertTrue(action1 instanceof ODiscoverAction.OEstablishAction);
    OTransactionIdPromise promiseId1 = new OTransactionIdPromise(nodeId1, new OTransactionId(1, 1));
    ops1.startEstablish(promiseId1, Set.of(nodeId1), new TestAction());
    ops1.validateEnstablish(groupId, Set.of(nodeId1));
    ops1.enstablish(groupId, Set.of(nodeId1));

    assertEquals(ops1.getMembers().size(), 1);
    assertTrue(ops1.getMembers().contains(nodeId1));

    // Second node quorum1 establish
    OCoordinatedDistributedOps ops2 = new OCoordinatedDistributedOpsImpl(nodeId2, groupId, 1);
    ODiscoverAction action2 = ops2.nodeJoinStart(nodeId2, OTopologyStateNetwork.boot(groupId));
    assertTrue(action2 instanceof ODiscoverAction.OEstablishAction);
    OTransactionIdPromise promiseId2 = new OTransactionIdPromise(nodeId2, new OTransactionId(1, 1));
    ops2.startEstablish(promiseId2, Set.of(nodeId2), new TestAction());
    ops2.validateEnstablish(groupId, Set.of(nodeId2));
    ops2.enstablish(groupId, Set.of(nodeId2));

    assertEquals(ops2.getMembers().size(), 1);
    assertTrue(ops2.getMembers().contains(nodeId2));

    // This case now should start a merge case of the network
    OTopologyStateNetwork state = ops2.getNetworkState();
    ODiscoverAction exectedMerge = ops1.nodeJoinStart(nodeId2, state);
    assertTrue(exectedMerge instanceof ODiscoverAction.OMergeAction);
  }
}
