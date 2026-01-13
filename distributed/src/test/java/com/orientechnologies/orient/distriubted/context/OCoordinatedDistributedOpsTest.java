package com.orientechnologies.orient.distriubted.context;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

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
import com.orientechnologies.orient.distributed.context.coordination.OOperationStart;
import com.orientechnologies.orient.distributed.context.coordination.action.OCompleteAction;
import com.orientechnologies.orient.distributed.context.coordination.dbs.ODatabaseState;
import com.orientechnologies.orient.distributed.context.coordination.dbs.ODatabaseStateChangeListener;
import com.orientechnologies.orient.distributed.context.coordination.message.state.ONodeStateNetwork;
import com.orientechnologies.orient.distributed.context.coordination.message.state.OTopologyStateNetwork;
import com.orientechnologies.orient.distributed.context.coordination.result.OAcceptResult;
import com.orientechnologies.orient.distributed.context.coordination.result.OInvalidSequential;
import com.orientechnologies.orient.distributed.context.coordination.result.OQuorumNotReached;
import com.orientechnologies.orient.distributed.context.coordination.topology.ODiscoverAction;
import com.orientechnologies.orient.distributed.context.coordination.topology.ODiscoverAction.OAddNodeAction;
import com.orientechnologies.orient.distributed.context.coordination.topology.OTopologyState;
import java.util.Collections;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import org.junit.Test;

public class OCoordinatedDistributedOpsTest
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

  @Test
  public void baseSuccess() {
    TestAction action = new TestAction();
    ONodeId nodeId = newRandomNodeId();
    OGroupId groupId = newRandomGroupId();
    OCoordinatedDistributedOps ops =
        new OCoordinatedDistributedOpsImpl(nodeId, groupId, 2, this, this);
    ops.registerNode(nodeId, 0);
    ONodeId nodeIdtwo = newRandomNodeId();
    ops.registerNode(nodeIdtwo, 0);

    var res = ops.start(action);
    var promise = res.get().promise();
    ops.nodeSuccess(nodeId, promise);
    ops.nodeSuccess(nodeIdtwo, promise);

    assertTrue(action.success);
    assertFalse(action.failure);
  }

  @Test
  public void baseSuccessComplete() {
    TestAction action = new TestAction();
    ONodeId nodeId = newRandomNodeId();
    OGroupId groupId = newRandomGroupId();
    OCoordinatedDistributedOps ops =
        new OCoordinatedDistributedOpsImpl(nodeId, groupId, 2, this, this);
    ops.registerNode(nodeId, 0);
    ONodeId nodeIdtwo = newRandomNodeId();
    ops.registerNode(nodeIdtwo, 0);

    var res = ops.start(action);
    var promise = res.get().promise();
    ops.nodeSuccess(nodeId, promise);
    ops.nodeSuccess(nodeIdtwo, promise);
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
    OCoordinatedDistributedOps ops =
        new OCoordinatedDistributedOpsImpl(nodeId, groupId, 2, this, this);
    ONodeId nodeIdtwo = newRandomNodeId();
    ops.establish(groupId, Set.of(nodeId, nodeIdtwo));

    var res = ops.start(action);
    var promise = res.get().promise();
    ops.nodeSuccess(nodeId, promise);
    ops.nodeFailure(nodeIdtwo, promise, new OInvalidSequential(0, 0));

    assertFalse(action.success);
    assertTrue(action.failure);
  }

  @Test
  public void baseSuccessMoreNodes() {
    TestAction action = new TestAction();
    ONodeId nodeId = newRandomNodeId();
    OGroupId groupId = new OGroupId("group");
    OCoordinatedDistributedOps ops =
        new OCoordinatedDistributedOpsImpl(nodeId, groupId, 2, this, this);
    ONodeId nodeIdtwo = newRandomNodeId();
    ONodeId nodeIdthree = newRandomNodeId();
    ONodeId nodeIdFour = newRandomNodeId();
    ops.establish(groupId, Set.of(nodeId, nodeIdtwo, nodeIdthree, nodeIdFour));

    var res = ops.start(action);
    var promise = res.get().promise();
    ops.nodeSuccess(nodeId, promise);
    ops.nodeSuccess(nodeIdtwo, promise);
    assertFalse(action.success);
    ops.nodeSuccess(nodeIdthree, promise);
    assertTrue(action.success);
    ops.nodeSuccess(nodeIdFour, promise);

    assertTrue(action.success);
    assertFalse(action.failure);
  }

  @Test
  public void baseSuccessMoreNodesUregistered() {
    TestAction action = new TestAction();
    ONodeId nodeId = newRandomNodeId();
    OGroupId groupId = newRandomGroupId();
    OCoordinatedDistributedOps ops =
        new OCoordinatedDistributedOpsImpl(nodeId, groupId, 2, this, this);
    ops.registerNode(nodeId, 0);
    ONodeId nodeIdtwo = newRandomNodeId();
    ops.registerNode(nodeIdtwo, 0);
    ONodeId nodeIdthree = newRandomNodeId();
    ops.registerNode(nodeIdthree, 0);
    ONodeId nodeIdFour = newRandomNodeId();
    ops.registerNode(nodeIdFour, 0);

    ops.unregisterNode(nodeIdthree, 0);
    ops.unregisterNode(nodeIdFour, 0);

    var res = ops.start(action);
    ops.nodeSuccess(nodeId, res.get().promise());
    ops.nodeSuccess(nodeIdtwo, res.get().promise());
    assertTrue(action.success);
    assertFalse(action.failure);
  }

  @Test
  public void baseFailureeNodesUregistered() throws InterruptedException, ExecutionException {
    TestAction action = new TestAction();
    ONodeId nodeId = newRandomNodeId();
    OGroupId groupId = new OGroupId("group");
    OCoordinatedDistributedOps ops =
        new OCoordinatedDistributedOpsImpl(nodeId, groupId, 2, this, this);
    ONodeId nodeIdtwo = newRandomNodeId();
    Set<ONodeId> nodes = Set.of(nodeId, nodeIdtwo);
    ops.establish(groupId, nodes);

    Optional<OOperationStart> start = ops.start(action);
    assertTrue(start.get().nodes().containsAll(nodes));
    ops.nodeSuccess(nodeId, start.get().promise());
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
    OCoordinatedDistributedOps ops =
        new OCoordinatedDistributedOpsImpl(nodeId, groupId, 2, this, this);
    ONodeId nodeIdtwo = newRandomNodeId();
    ONodeId nodeIdthree = newRandomNodeId();
    ONodeId nodeIdFour = newRandomNodeId();
    Set<ONodeId> nodes = Set.of(nodeId, nodeIdtwo, nodeIdthree, nodeIdFour);
    ops.establish(groupId, nodes);

    Optional<OOperationStart> start = ops.start(action);
    assertTrue(start.get().nodes().containsAll(nodes));
    OTransactionIdPromise promise = start.get().promise();
    ops.nodeSuccess(nodeId, promise);
    ops.nodeSuccess(nodeIdtwo, promise);
    ops.unregisterNode(nodeIdthree, 0);
    ops.nodeFailure(nodeIdFour, promise, new OInvalidSequential(0, 0));
    assertTrue(action.failure);
    assertFalse(action.success);
    assertTrue(action.result.get() instanceof OQuorumNotReached);
  }

  @Test
  public void baseNodeDiscoverEmpty() {
    ONodeId nodeId = newRandomNodeId();
    OGroupId groupId = newRandomGroupId();
    OCoordinatedDistributedOps ops =
        new OCoordinatedDistributedOpsImpl(nodeId, groupId, 2, this, this);
    ODiscoverAction action = ops.nodeJoinStart(nodeId, bootNetworkState(groupId), false);
    assertTrue(action instanceof ODiscoverAction.ONoneAction);
    assertTrue(ops.getNetworkMembers().isEmpty());
  }

  @Test
  public void baseNodeDiscoverReachQuorum() {
    ONodeId nodeId = newRandomNodeId();
    OGroupId groupId = newRandomGroupId();
    OCoordinatedDistributedOps ops =
        new OCoordinatedDistributedOpsImpl(nodeId, groupId, 2, this, this);
    ODiscoverAction action = ops.nodeJoinStart(nodeId, bootNetworkState(groupId), false);
    assertTrue(action instanceof ODiscoverAction.ONoneAction);

    ONodeId nodeId1 = newRandomNodeId();
    action = ops.nodeJoinStart(nodeId1, bootNetworkState(groupId), false);

    assertTrue(action instanceof ODiscoverAction.OEstablishAction);
    ops.establish(
        ((ODiscoverAction.OEstablishAction) action).groupId(),
        ((ODiscoverAction.OEstablishAction) action).candidates());

    assertEquals(ops.getNetworkMembers().size(), 2);
    assertEquals(ops.getNetworkState().topology().quorum(), 2);
  }

  @Test
  public void nodeDiscoverAddNode() {
    ONodeId nodeId = newRandomNodeId();
    OGroupId groupId = newRandomGroupId();
    OCoordinatedDistributedOps ops =
        new OCoordinatedDistributedOpsImpl(nodeId, groupId, 2, this, this);
    ODiscoverAction action = ops.nodeJoinStart(nodeId, bootNetworkState(groupId), false);

    assertTrue(action instanceof ODiscoverAction.ONoneAction);

    ONodeId nodeId1 = newRandomNodeId();
    action = ops.nodeJoinStart(nodeId1, bootNetworkState(groupId), false);

    assertTrue(action instanceof ODiscoverAction.OEstablishAction);
    ops.establish(
        ((ODiscoverAction.OEstablishAction) action).groupId(),
        ((ODiscoverAction.OEstablishAction) action).candidates());

    assertEquals(ops.getNetworkMembers().size(), 2);
    ONodeId nodeId2 = newRandomNodeId();
    action = ops.nodeJoinStart(nodeId2, bootNetworkState(groupId), false);
    assertTrue(action instanceof ODiscoverAction.OAddNodeAction);
    ops.registerNode(
        ((ODiscoverAction.OAddNodeAction) action).node(),
        ((ODiscoverAction.OAddNodeAction) action).version());
    assertEquals(ops.getNetworkMembers().size(), 3);
  }

  @Test
  public void threeNodesJoinCoordinationDefine() {
    OGroupId groupId = newRandomGroupId();
    ONodeId nodeId1 = newRandomNodeId();
    OCoordinatedDistributedOps node1 =
        new OCoordinatedDistributedOpsImpl(nodeId1, groupId, 2, this, this);
    ONodeId nodeId2 = newRandomNodeId();
    OCoordinatedDistributedOps node2 =
        new OCoordinatedDistributedOpsImpl(nodeId2, groupId, 2, this, this);
    ONodeId nodeId3 = newRandomNodeId();

    ODiscoverAction action = node1.nodeJoinStart(nodeId1, bootNetworkState(groupId), false);
    assertTrue(action instanceof ODiscoverAction.ONoneAction);

    action = node1.nodeJoinStart(nodeId2, bootNetworkState(groupId), false);
    assertTrue(action instanceof ODiscoverAction.OEstablishAction);
    var candidates = ((ODiscoverAction.OEstablishAction) action).candidates();
    var networkId = ((ODiscoverAction.OEstablishAction) action).groupId();
    Optional<OAcceptResult> result = node2.validateEstablish(networkId, candidates);
    assertTrue(result.isEmpty());
    assertTrue(result.isEmpty());

    node1.establish(networkId, candidates);
    node2.establish(networkId, candidates);
    assertEquals(node1.getNetworkMembers().size(), 2);
    assertEquals(node2.getNetworkMembers().size(), 2);

    action = node1.nodeJoinStart(nodeId3, bootNetworkState(groupId), false);
    assertTrue(action instanceof ODiscoverAction.OAddNodeAction);
    var addNode = ((ODiscoverAction.OAddNodeAction) action).node();
    var addVersion = ((ODiscoverAction.OAddNodeAction) action).version();
    assertTrue(addVersion > 0);
    var res = node1.validateRegisterNode(addNode, addVersion);
    assertTrue(res.isEmpty());
    res = node2.validateRegisterNode(addNode, addVersion);
    assertTrue(res.isEmpty());
    node1.registerNode(addNode, addVersion);
    node2.registerNode(addNode, addVersion);

    assertEquals(node1.getNetworkMembers().size(), 3);
    assertEquals(node2.getNetworkMembers().size(), 3);
  }

  @Test
  public void threeNodesJoinCoordinationFail() {
    OGroupId groupId = newRandomGroupId();
    ONodeId nodeId1 = newRandomNodeId();
    OCoordinatedDistributedOps node1 =
        new OCoordinatedDistributedOpsImpl(nodeId1, groupId, 2, this, this);
    ONodeId nodeId2 = newRandomNodeId();
    OCoordinatedDistributedOps node2 =
        new OCoordinatedDistributedOpsImpl(nodeId2, groupId, 2, this, this);
    ONodeId nodeId3 = newRandomNodeId();

    ODiscoverAction action = node1.nodeJoinStart(nodeId1, bootNetworkState(groupId), false);

    assertTrue(action instanceof ODiscoverAction.ONoneAction);

    action = node1.nodeJoinStart(nodeId2, bootNetworkState(groupId), false);
    assertTrue(action instanceof ODiscoverAction.OEstablishAction);
    var candidates = ((ODiscoverAction.OEstablishAction) action).candidates();
    var networkId = ((ODiscoverAction.OEstablishAction) action).groupId();
    Optional<OAcceptResult> result = node2.validateEstablish(networkId, candidates);
    assertTrue(result.isEmpty());
    assertTrue(result.isEmpty());

    node1.establish(networkId, candidates);
    node2.establish(networkId, candidates);
    assertEquals(node1.getNetworkMembers().size(), 2);
    assertEquals(node2.getNetworkMembers().size(), 2);

    action = node1.nodeJoinStart(nodeId3, bootNetworkState(groupId), false);
    assertTrue(action instanceof ODiscoverAction.OAddNodeAction);

    var addNode = ((ODiscoverAction.OAddNodeAction) action).node();
    var addVersion = ((ODiscoverAction.OAddNodeAction) action).version() - 1;

    var res = node1.validateRegisterNode(addNode, addVersion);
    assertFalse(res.isEmpty());
    res = node2.validateRegisterNode(addNode, addVersion);
    assertFalse(res.isEmpty());

    assertEquals(node1.getNetworkMembers().size(), 2);
    assertEquals(node2.getNetworkMembers().size(), 2);
    try {
      node1.getNetworkMembers().add(newRandomNodeId());
      fail();
    } catch (UnsupportedOperationException e) {
    }
  }

  @Test
  public void nodesJoinAlreadyDefinedMoreRecent() {
    ONodeId nodeId1 = newRandomNodeId();
    OGroupId groupId = newRandomGroupId();
    OCoordinatedDistributedOps node1 =
        new OCoordinatedDistributedOpsImpl(nodeId1, groupId, 2, this, this);
    ONodeId nodeId2 = newRandomNodeId();
    ONodeId nodeId3 = newRandomNodeId();
    var gid = newRandomGroupId();
    ONetworkTopologyStore initial =
        new ONetworkTopologyStore(gid, OTopologyState.ESTABLISHED, Set.of(nodeId1, nodeId2), 2, 2);
    node1.load(new ONodeStateStore(Optional.empty(), Optional.of(initial), Optional.empty()));
    node1.discoverNode(nodeId1);
    assertEquals(node1.getNetworkMembers().size(), 2);
    OTopologyStateNetwork establish =
        new OTopologyStateNetwork(
            gid, OTopologyState.ESTABLISHED, Set.of(nodeId1, nodeId2, nodeId3), 2, 3);

    ODiscoverAction action = node1.nodeJoinStart(nodeId2, networkState(establish), false);
    assertTrue(action instanceof ODiscoverAction.OApplyStateAction);

    assertEquals(node1.getNetworkMembers().size(), 3);
    try {
      node1.getNetworkMembers().add(newRandomNodeId());
      fail();
    } catch (UnsupportedOperationException e) {
    }
  }

  @Test
  public void nodesJoinAlreadyDefinedOutdated() {
    ONodeId nodeId1 = newRandomNodeId();
    OGroupId groupId = newRandomGroupId();
    OCoordinatedDistributedOps node1 =
        new OCoordinatedDistributedOpsImpl(nodeId1, groupId, 2, this, this);
    ONodeId nodeId2 = newRandomNodeId();
    ONodeId nodeId3 = newRandomNodeId();
    ONetworkTopologyStore initial =
        new ONetworkTopologyStore(
            groupId, OTopologyState.ESTABLISHED, Set.of(nodeId1, nodeId2, nodeId3), 2, 3);
    node1.load(new ONodeStateStore(Optional.empty(), Optional.of(initial), Optional.empty()));
    node1.discoverNode(nodeId1);
    assertEquals(node1.getNetworkMembers().size(), 3);
    assertEquals(node1.getNetworkState().topology().quorum(), 2);

    OTopologyStateNetwork enstablis =
        new OTopologyStateNetwork(
            groupId, OTopologyState.ESTABLISHED, Set.of(nodeId1, nodeId2), 2, 2);
    ODiscoverAction action = node1.nodeJoinStart(nodeId2, networkState(enstablis), false);
    assertTrue(action instanceof ODiscoverAction.ONotifySelf);

    assertEquals(node1.getNetworkMembers().size(), 3);
  }

  @Test
  public void nodesJoinAlreadyDefinedNewNode() {
    ONodeId nodeId1 = newRandomNodeId();
    OGroupId groupId = newRandomGroupId();
    OCoordinatedDistributedOps node1 =
        new OCoordinatedDistributedOpsImpl(nodeId1, groupId, 2, this, this);
    ONodeId nodeId2 = newRandomNodeId();
    ONodeId nodeId3 = newRandomNodeId();
    ONetworkTopologyStore initial =
        new ONetworkTopologyStore(
            groupId, OTopologyState.ESTABLISHED, Set.of(nodeId1, nodeId2), 2, 3);
    node1.load(new ONodeStateStore(Optional.empty(), Optional.of(initial), Optional.empty()));
    node1.discoverNode(nodeId1);
    assertEquals(node1.getNetworkMembers().size(), 2);

    ODiscoverAction action = node1.nodeJoinStart(nodeId3, bootNetworkState(groupId), false);
    assertTrue(action instanceof ODiscoverAction.OAddNodeAction);

    assertEquals(node1.getNetworkMembers().size(), 2);
  }

  @Test
  public void nodesNotDefineInoreTopology() {
    ONodeId nodeId1 = newRandomNodeId();
    OGroupId groupId = newRandomGroupId();
    OCoordinatedDistributedOps node1 =
        new OCoordinatedDistributedOpsImpl(nodeId1, groupId, 2, this, this);
    ONodeId nodeId2 = newRandomNodeId();
    ONodeId nodeId3 = newRandomNodeId();
    var gid = newRandomGroupId();
    ONetworkTopologyStore initial = bootStoreState(groupId);
    node1.load(new ONodeStateStore(Optional.empty(), Optional.of(initial), Optional.empty()));
    node1.discoverNode(nodeId1);
    assertEquals(node1.getNetworkMembers().size(), 0);

    OTopologyStateNetwork establish =
        new OTopologyStateNetwork(gid, OTopologyState.ESTABLISHED, Set.of(nodeId2, nodeId3), 2, 2);
    ODiscoverAction action = node1.nodeJoinStart(nodeId2, networkState(establish), false);
    assertTrue(action instanceof ODiscoverAction.ONoneAction);

    assertEquals(node1.getNetworkMembers().size(), 0);
  }

  @Test
  public void nodesBootGetDefinedTopology() {
    ONodeId nodeId1 = newRandomNodeId();
    OGroupId groupId = newRandomGroupId();
    OCoordinatedDistributedOps node1 =
        new OCoordinatedDistributedOpsImpl(nodeId1, groupId, 2, this, this);
    ONodeId nodeId2 = newRandomNodeId();
    ONetworkTopologyStore initial = bootStoreState(groupId);
    node1.load(new ONodeStateStore(Optional.empty(), Optional.of(initial), Optional.empty()));
    node1.discoverNode(nodeId1);
    assertEquals(node1.getNetworkMembers().size(), 0);

    OTopologyStateNetwork establish =
        new OTopologyStateNetwork(
            groupId, OTopologyState.ESTABLISHED, Set.of(nodeId1, nodeId2), 2, 2);
    ODiscoverAction action = node1.nodeJoinStart(nodeId2, networkState(establish), false);
    assertTrue(action instanceof ODiscoverAction.OApplyStateAction);

    assertEquals(node1.getNetworkMembers().size(), 2);
    try {
      node1.getNetworkMembers().add(newRandomNodeId());
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
    OCoordinatedDistributedOps ops1 =
        new OCoordinatedDistributedOpsImpl(nodeId1, groupId, 1, this, this);
    ODiscoverAction action1 = ops1.nodeJoinStart(nodeId1, bootNetworkState(groupId), false);
    assertTrue(action1 instanceof ODiscoverAction.OEstablishAction);
    ops1.startEstablish(Set.of(nodeId1), new TestAction());
    ops1.validateEstablish(groupId, Set.of(nodeId1));
    ops1.establish(groupId, Set.of(nodeId1));

    assertEquals(ops1.getNetworkMembers().size(), 1);
    assertTrue(ops1.getNetworkMembers().contains(nodeId1));

    // Second node quorum1 establish
    OCoordinatedDistributedOps ops2 =
        new OCoordinatedDistributedOpsImpl(nodeId2, groupId, 1, this, this);
    ODiscoverAction action2 = ops2.nodeJoinStart(nodeId2, bootNetworkState(groupId), false);
    assertTrue(action2 instanceof ODiscoverAction.OEstablishAction);
    ops2.startEstablish(Set.of(nodeId2), new TestAction());
    ops2.validateEstablish(groupId, Set.of(nodeId2));
    ops2.establish(groupId, Set.of(nodeId2));

    assertEquals(ops2.getNetworkMembers().size(), 1);
    assertTrue(ops2.getNetworkMembers().contains(nodeId2));

    // This case now should start a merge case of the network
    ONodeStateNetwork state = ops2.getNetworkState();
    ODiscoverAction exectedMerge = ops1.nodeJoinStart(nodeId2, state, false);
    assertTrue(exectedMerge instanceof ODiscoverAction.OMergeAction);

    ONodeStateNetwork mergeState = ops1.getNetworkState();
    ODiscoverAction addNode = ops2.nodeJoinStart(nodeId1, mergeState, true);
    assertTrue(addNode instanceof ODiscoverAction.OAddNodeAction);
    ODiscoverAction.OAddNodeAction add = (OAddNodeAction) addNode;
    ops2.validateRegisterNode(add.node(), add.version());
    ops2.registerNode(add.node(), add.version());
    assertEquals(ops2.getNetworkMembers().size(), 2);
    ops1.nodeJoinStart(nodeId2, ops2.getNetworkState(), false);
    assertEquals(ops1.getNetworkMembers().size(), 2);
  }
}
