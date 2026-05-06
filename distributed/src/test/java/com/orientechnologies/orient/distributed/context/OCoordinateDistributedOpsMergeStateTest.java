package com.orientechnologies.orient.distributed.context;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.orientechnologies.orient.core.transaction.ODatabaseId;
import com.orientechnologies.orient.core.transaction.OGroupId;
import com.orientechnologies.orient.core.transaction.ONodeId;
import com.orientechnologies.orient.core.transaction.OTransactionIdPromise;
import com.orientechnologies.orient.core.tx.OTransactionSequenceStatus;
import com.orientechnologies.orient.distributed.context.coordination.OCoordinatedDistributedOps;
import com.orientechnologies.orient.distributed.context.coordination.OCoordinatedDistributedOpsImpl;
import com.orientechnologies.orient.distributed.context.coordination.OResponseCollector;
import com.orientechnologies.orient.distributed.context.coordination.OResponseCollectorImpl;
import com.orientechnologies.orient.distributed.context.coordination.OResponseCollectorMerge;
import com.orientechnologies.orient.distributed.context.coordination.action.OCompleteAction;
import com.orientechnologies.orient.distributed.context.coordination.dbs.ODatabaseState;
import com.orientechnologies.orient.distributed.context.coordination.dbs.ODatabaseStateChangeListener;
import com.orientechnologies.orient.distributed.context.coordination.dbs.ONodeRole;
import com.orientechnologies.orient.distributed.context.coordination.message.operation.OAddNodeInfo;
import com.orientechnologies.orient.distributed.context.coordination.message.state.ONodeStateNetwork;
import com.orientechnologies.orient.distributed.context.coordination.message.state.OTopologyStateNetwork;
import com.orientechnologies.orient.distributed.context.coordination.result.OAcceptResult;
import com.orientechnologies.orient.distributed.context.coordination.topology.ODiscoverAction;
import java.util.Collections;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import org.junit.Test;

public class OCoordinateDistributedOpsMergeStateTest
    implements ODatabaseStateChangeListener, ONodeStateUpdated {

  @Override
  public void onStateChange(ODatabaseId dbId, ONodeId nodeId, ODatabaseState state) {}

  @Override
  public void update(ONodeStateStore newState) {}

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

  private class TestAction implements OCompleteAction {
    private ONodeId toMerge;

    public TestAction(ONodeId toMerge) {
      this.toMerge = toMerge;
    }

    @Override
    public void success(OTransactionIdPromise promise, Set<ONodeId> expected) {}

    @Override
    public void failure(
        OTransactionIdPromise promise, Set<ONodeId> expected, Optional<OAcceptResult> result) {}

    @Override
    public void complete(
        OTransactionIdPromise promise, Set<ONodeId> nodes, Optional<OAcceptResult> result) {}

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

  private void declareDatabase(
      OCoordinatedDistributedOps ops, ODatabaseId dbId, String databaseName) {
    var declare = ops.start(new TestAction(null));
    var promise = declare.get().promise();
    Set<OAddNodeInfo> parts = Set.of(new OAddNodeInfo(ops.getCurrent(), ONodeRole.Main));
    int minQuo = 1;
    var declareRes = ops.validateDeclareDatabase(promise, dbId, databaseName, parts, minQuo);
    assertTrue(declareRes.isEmpty());
    ops.consensusSuccess(promise);
    ops.declareDatabase(promise, dbId, databaseName, parts, minQuo);
    ops.completeExecution(promise);
  }

  private void setDatabaseState(
      OCoordinatedDistributedOps ops, ODatabaseId dbId, ONodeId node, ODatabaseState state) {
    var promiseData = ops.start(new TestAction(null));
    var statePromise = promiseData.get().promise();
    var stateVersion = ops.nextDatabaseVersion(dbId);
    ops.validateSetState(dbId, node, ODatabaseState.Online, stateVersion, statePromise);
    ops.consensusSuccess(statePromise);
    ops.setState(dbId, node, ODatabaseState.Online, stateVersion, statePromise);
    ops.completeExecution(statePromise);
  }

  @Test
  public void twoNodesNoDatabases() {

    ONodeId nodeId = newRandomNodeId();
    OGroupId groupId = newRandomGroupId();
    OCoordinatedDistributedOps ops = quorum1Env(nodeId, groupId);

    ONodeId nodeId1 = newRandomNodeId();

    OCoordinatedDistributedOps ops1 = quorum1Env(nodeId1, groupId);

    var state = ops.createMergedState(ops1.getNetworkState());

    assertTrue(state.topology().members().contains(nodeId));
    assertTrue(state.topology().members().contains(nodeId1));
    assertTrue(state.databases().isEmpty());
  }

  @Test
  public void twoNodesDatabaseOffline() {

    ONodeId nodeId = newRandomNodeId();
    OGroupId groupId = newRandomGroupId();
    OCoordinatedDistributedOps ops = quorum1Env(nodeId, groupId);

    ONodeId nodeId1 = newRandomNodeId();

    declareDatabase(ops, new ODatabaseId("testId"), "test");

    OCoordinatedDistributedOps ops1 = quorum1Env(nodeId1, groupId);
    var state = ops.createMergedState(ops1.getNetworkState());

    assertTrue(state.topology().members().contains(nodeId));
    assertTrue(state.topology().members().contains(nodeId1));
    assertEquals(state.databases().size(), 1);
    var db = state.databases().get(0);
    assertEquals(db.members().size(), 2);
    var member0 = db.members().get(0);
    assertEquals(member0.node(), nodeId);
    assertEquals(member0.state(), ODatabaseState.Offline);
    var member1 = db.members().get(1);
    assertEquals(member1.node(), nodeId1);
    assertEquals(member1.state(), ODatabaseState.Offline);
  }

  @Test
  public void twoNodesDatabaseOnline() {

    ONodeId nodeId = newRandomNodeId();
    OGroupId groupId = newRandomGroupId();
    OCoordinatedDistributedOps ops = quorum1Env(nodeId, groupId);

    ONodeId nodeId1 = newRandomNodeId();

    ODatabaseId dbId = new ODatabaseId("testId");
    declareDatabase(ops, dbId, "test");
    setDatabaseState(ops, dbId, nodeId, ODatabaseState.Online);

    OCoordinatedDistributedOps ops1 = quorum1Env(nodeId1, groupId);
    var state = ops.createMergedState(ops1.getNetworkState());

    assertTrue(state.topology().members().contains(nodeId));
    assertTrue(state.topology().members().contains(nodeId1));
    assertEquals(state.databases().size(), 1);
    var db = state.databases().get(0);
    assertEquals(db.members().size(), 2);
    var member0 = db.members().get(0);
    assertEquals(member0.node(), nodeId);
    assertEquals(member0.state(), ODatabaseState.Online);
    var member1 = db.members().get(1);
    assertEquals(member1.node(), nodeId1);
    assertEquals(member1.state(), ODatabaseState.Offline);
  }

  @Test
  public void twoNodesWithSameDatabaseOnline() {

    ONodeId nodeId = newRandomNodeId();
    OGroupId groupId = newRandomGroupId();
    OCoordinatedDistributedOps ops = quorum1Env(nodeId, groupId);

    ONodeId nodeId1 = newRandomNodeId();

    ODatabaseId dbId = new ODatabaseId("testId");
    declareDatabase(ops, dbId, "test");
    setDatabaseState(ops, dbId, nodeId, ODatabaseState.Online);

    OCoordinatedDistributedOps ops1 = quorum1Env(nodeId1, groupId);
    declareDatabase(ops1, dbId, "test");
    setDatabaseState(ops1, dbId, nodeId1, ODatabaseState.Online);
    var state = ops.createMergedState(ops1.getNetworkState());

    assertTrue(state.topology().members().contains(nodeId));
    assertTrue(state.topology().members().contains(nodeId1));
    assertEquals(state.databases().size(), 1);
    var db = state.databases().get(0);
    assertEquals(db.members().size(), 2);
    for (var member : db.members()) {
      assertEquals(member.state(), ODatabaseState.Online);
    }
  }
}
