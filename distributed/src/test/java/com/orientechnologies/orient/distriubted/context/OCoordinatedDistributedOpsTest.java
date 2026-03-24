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
import com.orientechnologies.orient.core.transaction.OTransactionId;
import com.orientechnologies.orient.core.transaction.OTransactionIdPromise;
import com.orientechnologies.orient.core.tx.OTransactionSequenceStatus;
import com.orientechnologies.orient.distributed.context.ONetworkTopologyStore;
import com.orientechnologies.orient.distributed.context.ONodeStateStore;
import com.orientechnologies.orient.distributed.context.ONodeStateUpdated;
import com.orientechnologies.orient.distributed.context.coordination.OCoordinatedDistributedOps;
import com.orientechnologies.orient.distributed.context.coordination.OCoordinatedDistributedOpsImpl;
import com.orientechnologies.orient.distributed.context.coordination.OOperationStart;
import com.orientechnologies.orient.distributed.context.coordination.OResponseCollector;
import com.orientechnologies.orient.distributed.context.coordination.OResponseCollectorImpl;
import com.orientechnologies.orient.distributed.context.coordination.OVersion;
import com.orientechnologies.orient.distributed.context.coordination.action.OCompleteAction;
import com.orientechnologies.orient.distributed.context.coordination.dbs.ODatabaseState;
import com.orientechnologies.orient.distributed.context.coordination.dbs.ODatabaseStateChangeListener;
import com.orientechnologies.orient.distributed.context.coordination.message.state.ONodeStateNetwork;
import com.orientechnologies.orient.distributed.context.coordination.message.state.OTopologyStateNetwork;
import com.orientechnologies.orient.distributed.context.coordination.result.OAcceptResult;
import com.orientechnologies.orient.distributed.context.coordination.result.OInvalidSequential;
import com.orientechnologies.orient.distributed.context.coordination.result.OQuorumNotReached;
import com.orientechnologies.orient.distributed.context.coordination.topology.ODiscoverAction;
import com.orientechnologies.orient.distributed.context.coordination.topology.ODiscoverAction.OMergeNodeAction;
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

    @Override
    public OResponseCollector newResponseCollector(
        OTransactionIdPromise promise, int quorum, Set<ONodeId> nodes) {
      return new OResponseCollectorImpl(this, promise, quorum, nodes);
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
    var prom = newPromiseId(nodeId);
    ops.registerNode(nodeId, new OVersion(0), prom);
    ONodeId nodeIdtwo = newRandomNodeId();
    ops.registerNode(nodeIdtwo, new OVersion(0), prom);

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
    var prom = newPromiseId(nodeId);
    ops.registerNode(nodeId, new OVersion(0), prom);
    ONodeId nodeIdtwo = newRandomNodeId();
    ops.registerNode(nodeIdtwo, new OVersion(0), prom);

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
    var enPromise = newPromiseId(nodeId);
    ops.establish(groupId, Set.of(nodeId, nodeIdtwo), enPromise);

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
    var enPromise = newPromiseId(nodeId);
    ops.establish(groupId, Set.of(nodeId, nodeIdtwo, nodeIdthree, nodeIdFour), enPromise);

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
    var promise = newPromiseId(nodeId);
    ops.registerNode(nodeId, new OVersion(0), promise);
    ONodeId nodeIdtwo = newRandomNodeId();
    ops.registerNode(nodeIdtwo, new OVersion(0), promise);
    ONodeId nodeIdthree = newRandomNodeId();
    ops.registerNode(nodeIdthree, new OVersion(0), promise);
    ONodeId nodeIdFour = newRandomNodeId();
    ops.registerNode(nodeIdFour, new OVersion(0), promise);

    ops.unregisterNode(nodeIdthree, new OVersion(0), promise);
    ops.unregisterNode(nodeIdFour, new OVersion(0), promise);

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
    var esPromise = newPromiseId(nodeId);
    ops.establish(groupId, nodes, esPromise);

    Optional<OOperationStart> start = ops.start(action);
    assertTrue(start.get().nodes().containsAll(nodes));
    ops.nodeSuccess(nodeId, start.get().promise());
    ops.unregisterNode(nodeIdtwo, new OVersion(0), esPromise);
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
    var esPromise = newPromiseId(nodeId);
    Set<ONodeId> nodes = Set.of(nodeId, nodeIdtwo, nodeIdthree, nodeIdFour);
    ops.establish(groupId, nodes, esPromise);

    Optional<OOperationStart> start = ops.start(action);
    assertTrue(start.get().nodes().containsAll(nodes));
    OTransactionIdPromise promise = start.get().promise();
    ops.nodeSuccess(nodeId, promise);
    ops.nodeSuccess(nodeIdtwo, promise);
    ops.unregisterNode(nodeIdthree, new OVersion(0), esPromise);
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
    assertTrue(ops.getNetworkTopology().getMembers().isEmpty());
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

    var esPromise = newPromiseId(nodeId1);
    assertTrue(action instanceof ODiscoverAction.OEstablishAction);
    ops.establish(
        ((ODiscoverAction.OEstablishAction) action).groupId(),
        ((ODiscoverAction.OEstablishAction) action).candidates(),
        esPromise);

    assertEquals(ops.getNetworkTopology().getMembers().size(), 2);
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

    var esPromise = newPromiseId(nodeId1);
    assertTrue(action instanceof ODiscoverAction.OEstablishAction);
    ops.establish(
        ((ODiscoverAction.OEstablishAction) action).groupId(),
        ((ODiscoverAction.OEstablishAction) action).candidates(),
        esPromise);

    assertEquals(ops.getNetworkTopology().getMembers().size(), 2);
    ONodeId nodeId2 = newRandomNodeId();
    action = ops.nodeJoinStart(nodeId2, bootNetworkState(groupId), false);
    var version = ops.nextTopologyVersion();
    assertTrue(action instanceof ODiscoverAction.OAddNodeAction);
    ops.registerNode(
        ((ODiscoverAction.OAddNodeAction) action).node(), version, newPromiseId(nodeId));
    assertEquals(ops.getNetworkTopology().getMembers().size(), 3);
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
    var enPromise = newPromiseId(nodeId1);
    Optional<OAcceptResult> result = node2.validateEstablish(networkId, candidates, enPromise);
    assertTrue(result.isEmpty());
    assertTrue(result.isEmpty());

    node1.establish(networkId, candidates, enPromise);
    node2.establish(networkId, candidates, enPromise);
    assertEquals(node1.getNetworkTopology().getMembers().size(), 2);
    assertEquals(node2.getNetworkTopology().getMembers().size(), 2);

    action = node1.nodeJoinStart(nodeId3, bootNetworkState(groupId), false);
    var addVersion = node1.nextTopologyVersion();
    assertTrue(action instanceof ODiscoverAction.OAddNodeAction);
    var addNode = ((ODiscoverAction.OAddNodeAction) action).node();
    assertTrue(addVersion.getValue() > 0);
    OTransactionIdPromise promise = newPromiseId(nodeId1);
    var res = node1.validateRegisterNode(addNode, addVersion, promise);
    assertTrue(res.isEmpty());
    res = node2.validateRegisterNode(addNode, addVersion, promise);
    assertTrue(res.isEmpty());
    node1.registerNode(addNode, addVersion, promise);
    node2.registerNode(addNode, addVersion, promise);

    assertEquals(node1.getNetworkTopology().getMembers().size(), 3);
    assertEquals(node2.getNetworkTopology().getMembers().size(), 3);
  }

  private OTransactionIdPromise newPromiseId(ONodeId nodeId) {
    return new OTransactionIdPromise(nodeId, new OTransactionId(10, 20));
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
    var enstablishPromise = newPromiseId(nodeId1);
    var candidates = ((ODiscoverAction.OEstablishAction) action).candidates();
    var networkId = ((ODiscoverAction.OEstablishAction) action).groupId();
    Optional<OAcceptResult> result =
        node2.validateEstablish(networkId, candidates, enstablishPromise);
    assertTrue(result.isEmpty());
    assertTrue(result.isEmpty());

    node1.establish(networkId, candidates, enstablishPromise);
    node2.establish(networkId, candidates, enstablishPromise);
    assertEquals(node1.getNetworkTopology().getMembers().size(), 2);
    assertEquals(node2.getNetworkTopology().getMembers().size(), 2);

    action = node1.nodeJoinStart(nodeId3, bootNetworkState(groupId), false);
    assertTrue(action instanceof ODiscoverAction.OAddNodeAction);

    var addNode = ((ODiscoverAction.OAddNodeAction) action).node();
    var addVersion = new OVersion(node1.nextTopologyVersion().getValue() - 1);
    OTransactionIdPromise promise = newPromiseId(nodeId1);
    var res = node1.validateRegisterNode(addNode, addVersion, promise);
    assertFalse(res.isEmpty());
    res = node2.validateRegisterNode(addNode, addVersion, promise);
    assertFalse(res.isEmpty());

    assertEquals(node1.getNetworkTopology().getMembers().size(), 2);
    assertEquals(node2.getNetworkTopology().getMembers().size(), 2);
    try {
      node1.getNetworkTopology().getMembers().add(newRandomNodeId());
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
    assertEquals(node1.getNetworkTopology().getMembers().size(), 2);
    OTopologyStateNetwork establish =
        new OTopologyStateNetwork(
            gid, OTopologyState.ESTABLISHED, Set.of(nodeId1, nodeId2, nodeId3), 2, 3);

    ODiscoverAction action = node1.nodeJoinStart(nodeId2, networkState(establish), false);
    assertTrue(action instanceof ODiscoverAction.OApplyStateAction);

    assertEquals(node1.getNetworkTopology().getMembers().size(), 3);
    try {
      node1.getNetworkTopology().getMembers().add(newRandomNodeId());
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
    assertEquals(node1.getNetworkTopology().getMembers().size(), 3);
    assertEquals(node1.getNetworkState().topology().quorum(), 2);

    OTopologyStateNetwork enstablis =
        new OTopologyStateNetwork(
            groupId, OTopologyState.ESTABLISHED, Set.of(nodeId1, nodeId2), 2, 2);
    ODiscoverAction action = node1.nodeJoinStart(nodeId2, networkState(enstablis), false);
    assertTrue(action instanceof ODiscoverAction.ONotifySelf);

    assertEquals(node1.getNetworkTopology().getMembers().size(), 3);
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
    assertEquals(node1.getNetworkTopology().getMembers().size(), 2);

    ODiscoverAction action = node1.nodeJoinStart(nodeId3, bootNetworkState(groupId), false);
    assertTrue(action instanceof ODiscoverAction.OAddNodeAction);

    assertEquals(node1.getNetworkTopology().getMembers().size(), 2);
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
    assertEquals(node1.getNetworkTopology().getMembers().size(), 0);

    OTopologyStateNetwork establish =
        new OTopologyStateNetwork(gid, OTopologyState.ESTABLISHED, Set.of(nodeId2, nodeId3), 2, 2);
    ODiscoverAction action = node1.nodeJoinStart(nodeId2, networkState(establish), false);
    assertTrue(action instanceof ODiscoverAction.ONoneAction);

    assertEquals(node1.getNetworkTopology().getMembers().size(), 0);
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
    assertEquals(node1.getNetworkTopology().getMembers().size(), 0);

    OTopologyStateNetwork establish =
        new OTopologyStateNetwork(
            groupId, OTopologyState.ESTABLISHED, Set.of(nodeId1, nodeId2), 2, 2);
    ODiscoverAction action = node1.nodeJoinStart(nodeId2, networkState(establish), false);
    assertTrue(action instanceof ODiscoverAction.OApplyStateAction);

    assertEquals(node1.getNetworkTopology().getMembers().size(), 2);
    try {
      node1.getNetworkTopology().getMembers().add(newRandomNodeId());
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
    var enProm = newPromiseId(nodeId1);
    ops1.validateEstablish(groupId, Set.of(nodeId1), enProm);
    ops1.establish(groupId, Set.of(nodeId1), enProm);

    assertEquals(ops1.getNetworkTopology().getMembers().size(), 1);
    assertTrue(ops1.getNetworkTopology().getMembers().contains(nodeId1));

    // Second node quorum1 establish
    OCoordinatedDistributedOps ops2 =
        new OCoordinatedDistributedOpsImpl(nodeId2, groupId, 1, this, this);
    ODiscoverAction action2 = ops2.nodeJoinStart(nodeId2, bootNetworkState(groupId), false);
    assertTrue(action2 instanceof ODiscoverAction.OEstablishAction);
    ops2.startEstablish(Set.of(nodeId2), new TestAction());
    var enProm2 = newPromiseId(nodeId2);
    ops2.validateEstablish(groupId, Set.of(nodeId2), enProm2);
    ops2.establish(groupId, Set.of(nodeId2), enProm2);

    assertEquals(ops2.getNetworkTopology().getMembers().size(), 1);
    assertTrue(ops2.getNetworkTopology().getMembers().contains(nodeId2));

    // This case now should start a merge case of the network
    ONodeStateNetwork state = ops2.getNetworkState();
    ODiscoverAction exectedMerge = ops1.nodeJoinStart(nodeId2, state, false);
    assertTrue(exectedMerge instanceof ODiscoverAction.ORequestMergeAction);

    ONodeStateNetwork mergeState = ops1.getNetworkState();
    ODiscoverAction addNode = ops2.nodeJoinStart(nodeId1, mergeState, true);
    var version = ops2.nextTopologyVersion();
    assertTrue(addNode instanceof OMergeNodeAction);
    OMergeNodeAction add = (OMergeNodeAction) addNode;
    OTransactionIdPromise promise = newPromiseId(nodeId1);
    ops2.validateRegisterNode(add.node(), version, promise);
    ops2.registerNode(add.node(), version, promise);
    assertEquals(ops2.getNetworkTopology().getMembers().size(), 2);
    ops1.nodeJoinStart(nodeId2, ops2.getNetworkState(), false);
    assertEquals(ops1.getNetworkTopology().getMembers().size(), 2);
  }
}
