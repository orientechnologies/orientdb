package com.orientechnologies.orient.distributed.context;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.log.OLogger;
import com.orientechnologies.orient.core.transaction.ODatabaseId;
import com.orientechnologies.orient.core.transaction.OGroupId;
import com.orientechnologies.orient.core.transaction.ONodeId;
import com.orientechnologies.orient.core.transaction.OTransactionId;
import com.orientechnologies.orient.core.transaction.OTransactionIdPromise;
import com.orientechnologies.orient.core.tx.OTransactionSequenceStatus;
import com.orientechnologies.orient.distributed.context.coordination.OCoordinatedDistributedOps;
import com.orientechnologies.orient.distributed.context.coordination.OCoordinatedDistributedOpsImpl;
import com.orientechnologies.orient.distributed.context.coordination.OResponseCollector;
import com.orientechnologies.orient.distributed.context.coordination.OResponseCollectorImpl;
import com.orientechnologies.orient.distributed.context.coordination.OResponseCollectorMerge;
import com.orientechnologies.orient.distributed.context.coordination.OVersion;
import com.orientechnologies.orient.distributed.context.coordination.action.OCompleteAction;
import com.orientechnologies.orient.distributed.context.coordination.dbs.ODatabaseState;
import com.orientechnologies.orient.distributed.context.coordination.dbs.ODatabaseStateChangeListener;
import com.orientechnologies.orient.distributed.context.coordination.dbs.ONodeRole;
import com.orientechnologies.orient.distributed.context.coordination.message.operation.OAddNodeInfo;
import com.orientechnologies.orient.distributed.context.coordination.message.state.ONodeStateNetwork;
import com.orientechnologies.orient.distributed.context.coordination.message.state.OTopologyStateNetwork;
import com.orientechnologies.orient.distributed.context.coordination.result.OAcceptResult;
import com.orientechnologies.orient.distributed.context.coordination.result.OAlreadyPromised;
import com.orientechnologies.orient.distributed.context.coordination.result.OCannotMerge;
import com.orientechnologies.orient.distributed.context.coordination.result.OInvalidSequential;
import com.orientechnologies.orient.distributed.context.coordination.topology.ODiscoverAction;
import com.orientechnologies.orient.distributed.context.coordination.topology.OTopologyState;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import org.junit.Test;

public class OCoordinatedDistributedOpsMergeTest
    implements ODatabaseStateChangeListener, ONodeStateUpdated {
  private static final OLogger logger =
      OLogManager.instance().logger(OCoordinatedDistributedOpsMergeTest.class);

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
    private int count = 0;
    private Optional<OAcceptResult> result;
    private ONodeId toMerge;

    public TestAction(ONodeId toMerge) {
      this.toMerge = toMerge;
    }

    @Override
    public void success(OTransactionIdPromise promise, Set<ONodeId> expected) {
      success = true;
      this.count += 1;
    }

    @Override
    public void failure(
        OTransactionIdPromise promise, Set<ONodeId> expected, Optional<OAcceptResult> result) {
      failure = true;
      this.result = result;
      this.count += 1;
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

  private ONodeStateNetwork networkState(OTopologyStateNetwork topology) {
    return new ONodeStateNetwork(
        topology, Collections.emptyList(), new OTransactionSequenceStatus(new long[] {}));
  }

  private ONodeStateNetwork bootNetworkState(OGroupId groupId) {
    return networkState(OTopologyStateNetwork.boot(groupId));
  }

  private OTransactionIdPromise newPromiseId(ONodeId nodeId) {
    return new OTransactionIdPromise(nodeId, new OTransactionId(1, 20));
  }

  private OCoordinatedDistributedOps quorum1Env(ONodeId nodeId, OGroupId groupId) {
    OCoordinatedDistributedOps ops =
        new OCoordinatedDistributedOpsImpl(nodeId, groupId, 1, this, this);
    ODiscoverAction action = ops.nodeJoinStart(nodeId, bootNetworkState(groupId), false);
    assertTrue(action instanceof ODiscoverAction.OEstablishAction);
    Optional<OTransactionIdPromise> seq = ops.startEstablish(Set.of(nodeId), new TestAction(null));
    ops.consensusSuccess(seq.get());
    ops.validateEstablish(groupId, Set.of(nodeId), seq.get());
    ops.establish(groupId, Set.of(nodeId), seq.get());
    ops.completeExecution(seq.get());
    return ops;
  }

  private void mergeNetworks(
      List<OCoordinatedDistributedOps> network, OCoordinatedDistributedOps toMerge) {
    var first = network.get(0);
    var promise = first.start(new TestAction(toMerge.getCurrent())).get().promise();
    ONodeStateNetwork toMergeOriginalState = toMerge.getNetworkState();
    var originalState = first.getNetworkState();
    var mergedState = first.createMergedState(toMergeOriginalState).get();
    for (var node : network) {
      var validate1 =
          node.validateMergeNode(toMerge.getCurrent(), mergedState, originalState, promise);
      assertTrue(validate1.isEmpty());
      first.nodeSuccess(node.getCurrent(), promise);
    }

    var validate2 =
        toMerge.validateMergeToNetwork(
            first.getGroupId(), mergedState, toMergeOriginalState, promise);
    first.nodeMergeResult(toMerge.getCurrent(), promise, validate2);
    assertTrue(validate2.isEmpty());
    for (var node : network) {
      node.consensusSuccess(promise);
      node.mergeNode(toMerge.getCurrent(), mergedState, originalState, promise);
      node.completeExecution(promise);
    }
    toMerge.mergeToNetwork(promise);
  }

  @Test
  public void baseCoordinationSuccess() {
    ONodeId nodeId1 = newRandomNodeId();
    OGroupId groupId = newRandomGroupId();

    OCoordinatedDistributedOps ops = quorum1Env(nodeId1, groupId);

    ONodeId nodeToMerge = newRandomNodeId();

    TestAction action = new TestAction(nodeToMerge);
    var res = ops.start(action);
    var promise = res.get().promise();

    ops.nodeSuccess(nodeId1, promise);
    assertFalse(action.success);
    assertFalse(action.failure);

    ops.nodeSuccess(nodeToMerge, promise);

    assertTrue(action.success);
    assertFalse(action.failure);
  }

  @Test
  public void baseCoordinationSuccessComplete() {
    ONodeId nodeId1 = newRandomNodeId();
    OGroupId groupId = newRandomGroupId();

    OCoordinatedDistributedOps ops = quorum1Env(nodeId1, groupId);

    ONodeId nodeToMerge = newRandomNodeId();

    TestAction action = new TestAction(nodeToMerge);
    var res = ops.start(action);
    var promise = res.get().promise();

    ops.nodeSuccess(nodeId1, promise);
    assertFalse(action.success);
    assertFalse(action.failure);

    ops.nodeSuccess(nodeToMerge, promise);

    assertTrue(action.success);
    assertFalse(action.failure);
    ops.completeExecution(promise);

    assertTrue(action.success);
    assertFalse(action.failure);
    assertTrue(action.complete);
  }

  @Test
  public void threeMergeCoordinationSuccessOneFailComplete() {
    ONodeId nodeId1 = newRandomNodeId();
    ONodeId nodeId2 = newRandomNodeId();
    ONodeId nodeId3 = newRandomNodeId();

    ONodeId nodeId4 = newRandomNodeId();

    OGroupId groupId = newRandomGroupId();

    OCoordinatedDistributedOps ops1 = quorum1Env(nodeId1, groupId);
    OCoordinatedDistributedOps ops2 = quorum1Env(nodeId2, groupId);
    OCoordinatedDistributedOps ops3 = quorum1Env(nodeId3, groupId);
    mergeNetworks(List.of(ops1), ops2);
    mergeNetworks(List.of(ops1, ops2), ops3);

    OCoordinatedDistributedOps ops4 = quorum1Env(nodeId4, groupId);

    TestAction action = new TestAction(ops4.getCurrent());
    var promise = ops1.start(action).get().promise();
    ONodeStateNetwork toMergeOriginalState = ops4.getNetworkState();
    var originalState = ops1.getNetworkState();
    var mergedState = ops1.createMergedState(toMergeOriginalState).get();

    var validate2 =
        ops4.validateMergeToNetwork(ops1.getGroupId(), mergedState, toMergeOriginalState, promise);
    ops1.nodeMergeResult(ops4.getCurrent(), promise, validate2);
    assertTrue(validate2.isEmpty());

    for (var node : List.of(ops1, ops2)) {
      var validate1 = node.validateMergeNode(nodeId4, mergedState, originalState, promise);
      assertTrue(validate1.isEmpty());
      ops1.nodeSuccess(node.getCurrent(), promise);
    }

    //    var validate1 = ops3.validateMergeNode(nodeId4, mergedState, version, promise);
    //    assertTrue(validate1.isEmpty());
    ops1.nodeFailure(ops3.getCurrent(), promise, new OInvalidSequential(10, 2));

    for (var node : List.of(ops1, ops2, ops3)) {
      node.consensusSuccess(promise);
      node.mergeNode(nodeId4, mergedState, originalState, promise);
      node.completeExecution(promise);
    }
    ops4.mergeToNetwork(promise);

    assertTrue(action.success);
    assertFalse(action.failure);
    assertEquals(action.count, 1);
    assertTrue(action.complete);
  }

  @Test
  public void threeMergeCoordinationSuccessComplete() {
    ONodeId nodeId1 = newRandomNodeId();
    ONodeId nodeId2 = newRandomNodeId();
    ONodeId nodeId3 = newRandomNodeId();

    ONodeId nodeId4 = newRandomNodeId();

    OGroupId groupId = newRandomGroupId();

    OCoordinatedDistributedOps ops1 = quorum1Env(nodeId1, groupId);
    OCoordinatedDistributedOps ops2 = quorum1Env(nodeId2, groupId);
    OCoordinatedDistributedOps ops3 = quorum1Env(nodeId3, groupId);
    mergeNetworks(List.of(ops1), ops2);
    mergeNetworks(List.of(ops1, ops2), ops3);

    OCoordinatedDistributedOps ops4 = quorum1Env(nodeId4, groupId);

    TestAction action = new TestAction(ops4.getCurrent());
    var promise = ops1.start(action).get().promise();
    ONodeStateNetwork toMergeOriginalState = ops4.getNetworkState();
    var originalState = ops1.getNetworkState();
    var mergedState = ops1.createMergedState(toMergeOriginalState).get();

    var validate2 =
        ops4.validateMergeToNetwork(ops1.getGroupId(), mergedState, toMergeOriginalState, promise);
    ops1.nodeMergeResult(ops4.getCurrent(), promise, validate2);
    assertTrue(validate2.isEmpty());

    for (var node : List.of(ops1, ops2, ops3)) {
      var validate1 = node.validateMergeNode(nodeId4, mergedState, originalState, promise);
      assertTrue(validate1.isEmpty());
      ops1.nodeSuccess(node.getCurrent(), promise);
    }

    for (var node : List.of(ops1, ops2, ops3)) {
      node.consensusSuccess(promise);
      node.mergeNode(nodeId4, mergedState, originalState, promise);
      node.completeExecution(promise);
    }
    ops4.mergeToNetwork(promise);

    assertTrue(action.success);
    assertFalse(action.failure);
    assertEquals(action.count, 1);
    assertTrue(action.complete);
  }

  @Test
  public void baseCoordinationFail() {
    ONodeId nodeId1 = newRandomNodeId();
    OGroupId groupId = newRandomGroupId();

    OCoordinatedDistributedOps ops = quorum1Env(nodeId1, groupId);

    ONodeId nodeToMerge = newRandomNodeId();

    TestAction action = new TestAction(nodeToMerge);
    var res = ops.start(action);
    var promise = res.get().promise();

    ops.nodeSuccess(nodeId1, promise);
    assertFalse(action.success);
    assertFalse(action.failure);
    ops.nodeFailure(nodeToMerge, promise, new OAlreadyPromised(nodeId1));

    assertFalse(action.success);
    assertTrue(action.failure);
  }

  @Test
  public void baseMergeCoordinationSucessTest() {
    ONodeId nodeId1 = newRandomNodeId();
    ONodeId nodeId2 = newRandomNodeId();
    OGroupId groupId = newRandomGroupId();

    OCoordinatedDistributedOps ops1 = quorum1Env(nodeId1, groupId);

    TestAction action = new TestAction(nodeId2);
    var res = ops1.start(action);
    var promise = res.get().promise();
    ops1.nodeSuccess(nodeId1, promise);
    ONodeStateNetwork nsn = newNetworkState(nodeId1, groupId);

    OCoordinatedDistributedOps ops2 = quorum1Env(nodeId2, groupId);
    var res1 = ops2.validateMergeToNetwork(groupId, nsn, ops1.getNetworkState(), promise);
    assertTrue(res1.isEmpty());
    ops1.nodeMergeResult(nodeId2, promise, res1);
    assertTrue(action.success);
    assertFalse(action.failure);
  }

  private ONodeStateNetwork newNetworkState(ONodeId nodeId1, OGroupId groupId) {
    OTopologyStateNetwork topology =
        new OTopologyStateNetwork(
            groupId, OTopologyState.ESTABLISHED, Set.of(nodeId1), 1, new OVersion(1));
    OTransactionSequenceStatus status = new OTransactionSequenceStatus(new long[] {});
    return new ONodeStateNetwork(topology, Collections.emptyList(), status);
  }

  @Test
  public void baseMergeDoublePromiseFailTest() {
    ONodeId nodeId1 = newRandomNodeId();
    ONodeId nodeId2 = newRandomNodeId();
    ONodeId nodeId3 = newRandomNodeId();
    OGroupId groupId = newRandomGroupId();

    OCoordinatedDistributedOps ops1 = quorum1Env(nodeId1, groupId);

    TestAction action = new TestAction(nodeId2);
    var res = ops1.start(action);
    var promise = res.get().promise();
    ops1.nodeSuccess(nodeId1, promise);

    OCoordinatedDistributedOps ops3 = quorum1Env(nodeId3, groupId);

    TestAction action1 = new TestAction(nodeId2);
    var res1 = ops3.start(action1);
    var promise1 = res1.get().promise();
    ops3.nodeSuccess(nodeId3, promise1);
    ONodeStateNetwork nsn = newNetworkState(nodeId1, groupId);

    OCoordinatedDistributedOps ops2 = quorum1Env(nodeId2, groupId);
    var res2 = ops2.validateMergeToNetwork(groupId, nsn, ops2.getNetworkState(), promise);
    assertTrue(res2.isEmpty());
    var res21 = ops2.validateMergeToNetwork(groupId, nsn, ops2.getNetworkState(), promise);
    assertFalse(res21.isEmpty());

    ops1.nodeMergeResult(nodeId2, promise, res2);
    assertTrue(action.success);
    assertFalse(action.failure);

    ops3.nodeMergeResult(nodeId2, promise1, res21);
    assertFalse(action1.success);
    assertTrue(action1.failure);
  }

  @Test
  public void twoNodesMergeFlowTest() {
    ONodeId nodeId1 = newRandomNodeId();
    ONodeId nodeId2 = newRandomNodeId();
    OGroupId groupId = newRandomGroupId();

    OCoordinatedDistributedOps ops1 = quorum1Env(nodeId1, groupId);
    OCoordinatedDistributedOps ops2 = quorum1Env(nodeId2, groupId);

    var promise = newPromiseId(nodeId1);
    ONodeStateNetwork ops2Original = ops2.getNetworkState();
    var mergedState = ops1.createMergedState(ops2Original).get();
    var validate1 = ops1.validateMergeNode(nodeId2, mergedState, ops1.getNetworkState(), promise);

    assertTrue(validate1.isEmpty());
    var validate2 = ops2.validateMergeToNetwork(groupId, mergedState, ops2Original, promise);
    assertTrue(validate2.isEmpty());
    ops1.mergeNode(nodeId2, mergedState, ops2.getNetworkState(), promise);
    ops2.mergeToNetwork(promise);
    assertTrue(ops1.getNetworkTopology().getMembers().contains(nodeId1));
    assertTrue(ops1.getNetworkTopology().getMembers().contains(nodeId2));

    assertTrue(ops2.getNetworkTopology().getMembers().contains(nodeId1));
    assertTrue(ops2.getNetworkTopology().getMembers().contains(nodeId2));

    var first = ops1.getTransactionSequenceState().getStatus();
    assertArrayEquals(first, ops2.getTransactionSequenceState().getStatus());

    var topologyv = ops1.getNetworkTopology().getVersion();
    assertEquals(topologyv, ops2.getNetworkTopology().getVersion());

    var topologyQ = ops1.getNetworkTopology().getQuorum();
    assertEquals(topologyQ, ops2.getNetworkTopology().getQuorum());
  }

  @Test
  public void twreeNodesMergeFlowTest() {
    ONodeId nodeId1 = newRandomNodeId();
    ONodeId nodeId2 = newRandomNodeId();
    ONodeId nodeId3 = newRandomNodeId();
    OGroupId groupId = newRandomGroupId();

    OCoordinatedDistributedOps ops1 = quorum1Env(nodeId1, groupId);
    OCoordinatedDistributedOps ops2 = quorum1Env(nodeId2, groupId);

    var promise = newPromiseId(nodeId1);
    ONodeStateNetwork ops2Original = ops2.getNetworkState();
    var mergedState = ops1.createMergedState(ops2Original).get();
    ONodeStateNetwork ops1Original = ops1.getNetworkState();
    var validate1 = ops1.validateMergeNode(nodeId2, mergedState, ops1Original, promise);

    assertTrue(validate1.isEmpty());
    var validate2 = ops2.validateMergeToNetwork(groupId, mergedState, ops2Original, promise);
    assertTrue(validate2.isEmpty());
    ops1.mergeNode(nodeId2, mergedState, ops1Original, promise);
    ops2.mergeToNetwork(promise);
    assertTrue(ops1.getNetworkTopology().getMembers().contains(nodeId1));
    assertTrue(ops1.getNetworkTopology().getMembers().contains(nodeId2));
    assertTrue(ops2.getNetworkTopology().getMembers().contains(nodeId1));
    assertTrue(ops2.getNetworkTopology().getMembers().contains(nodeId2));

    OCoordinatedDistributedOps ops3 = quorum1Env(nodeId3, groupId);

    ONodeStateNetwork ops3Original = ops3.getNetworkState();
    var secMergedState = ops2.createMergedState(ops3Original).get();
    var secPromise = newPromiseId(nodeId2);
    ONodeStateNetwork ops2Original1 = ops2.getNetworkState();
    var secValidate = ops2.validateMergeNode(nodeId3, secMergedState, ops2Original1, secPromise);
    assertTrue(secValidate.isEmpty());
    var secValidate1 = ops1.validateMergeNode(nodeId3, secMergedState, ops2Original1, secPromise);
    assertTrue(secValidate1.isEmpty());
    var secValidate2 =
        ops3.validateMergeToNetwork(groupId, secMergedState, ops3Original, secPromise);
    assertTrue(secValidate2.isEmpty());

    ops1.mergeNode(nodeId3, secMergedState, ops2Original1, secPromise);
    ops2.mergeNode(nodeId3, secMergedState, ops2Original1, secPromise);
    ops3.mergeToNetwork(secPromise);
    assertTrue(ops1.getNetworkTopology().getMembers().contains(nodeId1));
    assertTrue(ops1.getNetworkTopology().getMembers().contains(nodeId2));
    assertTrue(ops1.getNetworkTopology().getMembers().contains(nodeId3));

    assertTrue(ops2.getNetworkTopology().getMembers().contains(nodeId1));
    assertTrue(ops2.getNetworkTopology().getMembers().contains(nodeId2));
    assertTrue(ops2.getNetworkTopology().getMembers().contains(nodeId3));

    assertTrue(ops3.getNetworkTopology().getMembers().contains(nodeId1));
    assertTrue(ops3.getNetworkTopology().getMembers().contains(nodeId2));
    assertTrue(ops3.getNetworkTopology().getMembers().contains(nodeId3));
  }

  @Test
  public void twreeNodesMergeFlowOpsConflictTest() {
    ONodeId nodeId1 = newRandomNodeId();
    ONodeId nodeId2 = newRandomNodeId();
    ONodeId nodeId3 = newRandomNodeId();
    OGroupId groupId = newRandomGroupId();

    OCoordinatedDistributedOps ops1 = quorum1Env(nodeId1, groupId);
    OCoordinatedDistributedOps ops2 = quorum1Env(nodeId2, groupId);

    var promise = ops1.start(new TestAction(null)).get().promise();
    ONodeStateNetwork ops2Original = ops2.getNetworkState();
    var mergedState = ops1.createMergedState(ops2Original).get();
    ONodeStateNetwork ops1Original = ops1.getNetworkState();
    var validate1 = ops1.validateMergeNode(nodeId2, mergedState, ops1Original, promise);

    assertTrue(validate1.isEmpty());
    var validate2 = ops2.validateMergeToNetwork(groupId, mergedState, ops2Original, promise);
    assertTrue(validate2.isEmpty());
    ops1.consensusSuccess(promise);
    ops1.mergeNode(nodeId2, mergedState, ops1Original, promise);
    ops2.mergeToNetwork(promise);
    assertTrue(ops1.getNetworkTopology().getMembers().contains(nodeId1));
    assertTrue(ops1.getNetworkTopology().getMembers().contains(nodeId2));
    assertTrue(ops2.getNetworkTopology().getMembers().contains(nodeId1));
    assertTrue(ops2.getNetworkTopology().getMembers().contains(nodeId2));

    OCoordinatedDistributedOps ops3 = quorum1Env(nodeId3, groupId);

    var declare = ops3.start(new TestAction(null));
    var declarePromise = declare.get().promise();
    var dbId = new ODatabaseId("aaaa");
    String databaseName = "abc";
    Set<OAddNodeInfo> parts = Set.of(new OAddNodeInfo(nodeId3, ONodeRole.Main));
    int minQuo = 1;
    var declareRes =
        ops3.validateDeclareDatabase(declarePromise, dbId, databaseName, parts, minQuo);
    assertTrue(declareRes.isEmpty());
    ops3.consensusSuccess(declarePromise);
    ops3.declareDatabase(declarePromise, dbId, databaseName, parts, minQuo);

    ONodeStateNetwork ops3Original = ops3.getNetworkState();
    var secMergedState = ops2.createMergedState(ops3Original).get();
    var secPromise = ops2.start(new TestAction(null)).get().promise();
    var ops2Original1 = ops2.getNetworkState();
    var secValidate = ops2.validateMergeNode(nodeId3, secMergedState, ops2Original1, secPromise);
    assertTrue(secValidate.isEmpty());
    var secValidate1 = ops1.validateMergeNode(nodeId3, secMergedState, ops2Original1, secPromise);
    assertTrue(secValidate1.isEmpty());

    var state = ops3.start(new TestAction(null));
    var statePromise = state.get().promise();
    var stateVersion = ops3.nextDatabaseVersion(dbId);
    ops3.validateSetState(dbId, nodeId3, ODatabaseState.Online, stateVersion, statePromise);
    ops3.consensusSuccess(statePromise);
    ops3.setState(dbId, nodeId3, ODatabaseState.Online, stateVersion, statePromise);
    ops3.completeExecution(statePromise);

    var secValidate2 =
        ops3.validateMergeToNetwork(groupId, secMergedState, ops3Original, secPromise);
    assertTrue(secValidate2.isPresent());
    ops1.consensusFailure(secPromise);
    ops2.consensusFailure(secPromise);

    // Restart second round
    var newState = ((OCannotMerge) secValidate2.get()).currentState();

    var secMergedState1 = ops2.createMergedState(newState).get();
    var secPromise1 = newPromiseId(nodeId2);
    var ops2Original2 = ops2.getNetworkState();
    var secValidate1_1 =
        ops2.validateMergeNode(nodeId3, secMergedState1, ops2Original2, secPromise1);
    assertTrue(secValidate1_1.isEmpty());
    var secValidate1_2 =
        ops1.validateMergeNode(nodeId3, secMergedState1, ops2Original2, secPromise1);
    assertTrue(secValidate1_2.isEmpty());

    var secValidate1_3 =
        ops3.validateMergeToNetwork(groupId, secMergedState1, newState, secPromise1);
    assertTrue(secValidate1_3.isEmpty());

    ops1.consensusSuccess(secPromise1);
    ops1.mergeNode(nodeId3, secMergedState1, ops2Original2, secPromise1);
    ops2.consensusSuccess(secPromise1);
    ops2.mergeNode(nodeId3, secMergedState1, ops2Original2, secPromise1);
    ops3.mergeToNetwork(secPromise1);
    assertTrue(ops1.getNetworkTopology().getMembers().contains(nodeId1));
    assertTrue(ops1.getNetworkTopology().getMembers().contains(nodeId2));
    assertTrue(ops1.getNetworkTopology().getMembers().contains(nodeId3));

    assertTrue(ops2.getNetworkTopology().getMembers().contains(nodeId1));
    assertTrue(ops2.getNetworkTopology().getMembers().contains(nodeId2));
    assertTrue(ops2.getNetworkTopology().getMembers().contains(nodeId3));

    assertTrue(ops3.getNetworkTopology().getMembers().contains(nodeId1));
    assertTrue(ops3.getNetworkTopology().getMembers().contains(nodeId2));
    assertTrue(ops3.getNetworkTopology().getMembers().contains(nodeId3));

    var first = ops1.getTransactionSequenceState().getStatus();
    assertArrayEquals(first, ops2.getTransactionSequenceState().getStatus());
    assertArrayEquals(first, ops3.getTransactionSequenceState().getStatus());

    var dbv = ops1.getDatabaseTopology().getDatabaseVersion(dbId);
    assertEquals(dbv, ops2.getDatabaseTopology().getDatabaseVersion(dbId));
    assertEquals(dbv, ops3.getDatabaseTopology().getDatabaseVersion(dbId));

    var topologyv = ops1.getNetworkTopology().getVersion();
    assertEquals(topologyv, ops2.getNetworkTopology().getVersion());
    assertEquals(topologyv, ops3.getNetworkTopology().getVersion());

    var topologyQ = ops1.getNetworkTopology().getQuorum();
    assertEquals(topologyQ, ops2.getNetworkTopology().getQuorum());
    assertEquals(topologyQ, ops3.getNetworkTopology().getQuorum());
  }
}
