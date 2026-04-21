package com.orientechnologies.orient.distributed.context;

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
import com.orientechnologies.orient.distributed.context.coordination.OOperationStart;
import com.orientechnologies.orient.distributed.context.coordination.OResponseCollector;
import com.orientechnologies.orient.distributed.context.coordination.OResponseCollectorImpl;
import com.orientechnologies.orient.distributed.context.coordination.action.OCompleteAction;
import com.orientechnologies.orient.distributed.context.coordination.dbs.ODatabaseState;
import com.orientechnologies.orient.distributed.context.coordination.dbs.ODatabaseStateChangeListener;
import com.orientechnologies.orient.distributed.context.coordination.dbs.ONodeRole;
import com.orientechnologies.orient.distributed.context.coordination.message.OProposeOp;
import com.orientechnologies.orient.distributed.context.coordination.message.operation.OAddNodeInfo;
import com.orientechnologies.orient.distributed.context.coordination.message.operation.ODeclareDbMessage;
import com.orientechnologies.orient.distributed.context.coordination.message.state.ONodeStateNetwork;
import com.orientechnologies.orient.distributed.context.coordination.message.state.OTopologyStateNetwork;
import com.orientechnologies.orient.distributed.context.coordination.result.OAcceptResult;
import com.orientechnologies.orient.distributed.context.coordination.topology.ODiscoverAction;
import com.orientechnologies.orient.distributed.context.coordination.topology.ODiscoverAction.OMergeNodeAction;
import java.util.Collections;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import org.junit.Test;

public class OCoordinatedDistributedOpsRetryTest
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

    public TestAction() {}

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

  private ONodeStateNetwork networkState(OTopologyStateNetwork topology) {
    return new ONodeStateNetwork(
        topology, Collections.emptyList(), new OTransactionSequenceStatus(new long[] {}));
  }

  private ONodeStateNetwork bootNetworkState(OGroupId groupId) {
    return networkState(OTopologyStateNetwork.boot(groupId));
  }

  private OTransactionIdPromise newPromiseId(ONodeId nodeId) {
    return new OTransactionIdPromise(nodeId, new OTransactionId(10, 20));
  }

  private OCoordinatedDistributedOps quorum1Env(ONodeId nodeId, OGroupId groupId) {
    OCoordinatedDistributedOps ops =
        new OCoordinatedDistributedOpsImpl(nodeId, groupId, 1, this, this);
    ODiscoverAction action = ops.nodeJoinStart(nodeId, bootNetworkState(groupId), false);
    assertTrue(action instanceof ODiscoverAction.OEstablishAction);
    Optional<OTransactionIdPromise> seq = ops.startEstablish(Set.of(nodeId), new TestAction());
    ops.consensusSuccess(seq.get());
    var enPromise = newPromiseId(nodeId);
    ops.validateEstablish(groupId, Set.of(nodeId), enPromise);
    ops.establish(groupId, Set.of(nodeId), enPromise);
    return ops;
  }

  private void mergeEnvs(OCoordinatedDistributedOps ops1, OCoordinatedDistributedOps ops2) {
    ONodeStateNetwork state = ops2.getNetworkState();
    ODiscoverAction exectedMerge = ops1.nodeJoinStart(ops2.getCurrent(), state, false);
    assertTrue(exectedMerge instanceof ODiscoverAction.ORequestMergeAction);
    ONodeStateNetwork mergeState = ops1.getNetworkState();
    ODiscoverAction addNode = ops2.nodeJoinStart(ops1.getCurrent(), mergeState, true);
    var version = ops2.nextTopologyVersion();
    assertTrue(addNode instanceof OMergeNodeAction);
    OMergeNodeAction add = (OMergeNodeAction) addNode;
    OTransactionIdPromise promise = newPromiseId(ops1.getCurrent());
    ops2.validateRegisterNode(add.node(), version, promise);
    ops2.registerNode(add.node(), version, promise);
    ops1.nodeJoinStart(ops2.getCurrent(), ops2.getNetworkState(), false);
  }

  private OAddNodeInfo add(ONodeId node) {
    return new OAddNodeInfo(node, ONodeRole.Main);
  }

  @Test
  public void simpleRetrySuccess() {
    ONodeId nodeId1 = newRandomNodeId();
    ONodeId nodeId2 = newRandomNodeId();
    ONodeId nodeId3 = newRandomNodeId();
    OGroupId groupId = newRandomGroupId();

    // First node quorum1 establish
    OCoordinatedDistributedOps ops1 = quorum1Env(nodeId1, groupId);

    // Second node quorum1 establish
    OCoordinatedDistributedOps ops2 = quorum1Env(nodeId2, groupId);

    OCoordinatedDistributedOps ops3 = quorum1Env(nodeId3, groupId);

    mergeEnvs(ops2, ops1);
    mergeEnvs(ops3, ops1);
    ops2.nodeJoinStart(ops3.getCurrent(), ops3.getNetworkState(), false);

    Set<ONodeId> members = Set.of(nodeId1, nodeId2, nodeId3);

    var message =
        new ODeclareDbMessage(
            "db1",
            new ODatabaseId(),
            members.stream().map(this::add).collect(Collectors.toSet()),
            2);
    Optional<OOperationStart> sarted = ops1.start(new TestAction());

    OTransactionIdPromise promise = sarted.get().promise();
    var toReceive = new OProposeOp(promise, message);

    var res_1 = ops1.receive(toReceive);
    assertTrue(res_1.isEmpty());
    var res1 =
        ops1.validateDeclareDatabase(
            promise,
            message.getId(),
            message.getName(),
            message.getPartecipants(),
            message.getMinimumQuorum());
    assertTrue(res1.isEmpty());

    var res_2 = ops2.receive(toReceive);
    assertTrue(res_2.isEmpty());
    var res2 =
        ops2.validateDeclareDatabase(
            promise,
            message.getId(),
            message.getName(),
            message.getPartecipants(),
            message.getMinimumQuorum());
    assertTrue(res2.isEmpty());

    var res_3 = ops3.receive(toReceive);
    assertTrue(res_3.isEmpty());
    var res3 =
        ops3.validateDeclareDatabase(
            promise,
            message.getId(),
            message.getName(),
            message.getPartecipants(),
            message.getMinimumQuorum());
    assertTrue(res3.isEmpty());

    var rePromise = promise.retrySequence(nodeId2);
    var action = new TestAction();
    Optional<OOperationStart> resarted = ops2.restart(rePromise, action);
    OTransactionIdPromise repromise = resarted.get().promise();

    var res_21 = ops2.receiveRetry(repromise);
    assertTrue(res_21.isEmpty());
    var res21 =
        ops2.validateDeclareDatabase(
            repromise,
            message.getId(),
            message.getName(),
            message.getPartecipants(),
            message.getMinimumQuorum());
    assertTrue(res21.isEmpty());

    var res_31 = ops3.receiveRetry(repromise);
    assertTrue(res_31.isEmpty());
    var res31 =
        ops3.validateDeclareDatabase(
            repromise,
            message.getId(),
            message.getName(),
            message.getPartecipants(),
            message.getMinimumQuorum());
    assertTrue(res31.isEmpty());

    ops2.nodeSuccess(nodeId2, repromise);
    ops2.nodeSuccess(nodeId3, repromise);
    assertTrue(action.success);

    // This case now should start a merge case of the network
  }
}
