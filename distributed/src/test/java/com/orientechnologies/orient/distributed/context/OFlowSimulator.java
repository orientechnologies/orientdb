package com.orientechnologies.orient.distributed.context;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.orientechnologies.common.util.ORawPair;
import com.orientechnologies.orient.core.transaction.ODatabaseId;
import com.orientechnologies.orient.core.transaction.OGroupId;
import com.orientechnologies.orient.core.transaction.ONodeId;
import com.orientechnologies.orient.core.transaction.OTransactionIdPromise;
import com.orientechnologies.orient.core.tx.OTransactionSequenceStatus;
import com.orientechnologies.orient.distributed.context.coordination.OCoordinatedDistributedOps;
import com.orientechnologies.orient.distributed.context.coordination.OCoordinatedDistributedOpsImpl;
import com.orientechnologies.orient.distributed.context.coordination.OResponseCollector;
import com.orientechnologies.orient.distributed.context.coordination.OResponseCollectorImpl;
import com.orientechnologies.orient.distributed.context.coordination.OVersion;
import com.orientechnologies.orient.distributed.context.coordination.action.OCompleteAction;
import com.orientechnologies.orient.distributed.context.coordination.dbs.ODatabaseState;
import com.orientechnologies.orient.distributed.context.coordination.dbs.ODatabaseStateChangeListener;
import com.orientechnologies.orient.distributed.context.coordination.message.ODistributedMessage;
import com.orientechnologies.orient.distributed.context.coordination.message.operation.OAddNodeInfo;
import com.orientechnologies.orient.distributed.context.coordination.message.operation.OAddTopologyMember;
import com.orientechnologies.orient.distributed.context.coordination.message.operation.OEstablishTopology;
import com.orientechnologies.orient.distributed.context.coordination.message.operation.OOperationContext;
import com.orientechnologies.orient.distributed.context.coordination.message.operation.OOperationMessage;
import com.orientechnologies.orient.distributed.context.coordination.message.state.ONodeStateNetwork;
import com.orientechnologies.orient.distributed.context.coordination.message.state.OTopologyStateNetwork;
import com.orientechnologies.orient.distributed.context.coordination.result.OAcceptResult;
import com.orientechnologies.orient.distributed.context.coordination.topology.ODiscoverAction;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

public class OFlowSimulator implements ODatabaseStateChangeListener, ONodeStateUpdated {

  private OGroupId group = new OGroupId(UUID.randomUUID().toString());
  private List<ONodeId> nodes = new ArrayList<>();
  private Map<ONodeId, TestOperationContext> contexts = new HashMap<>();
  private final int networkInitQuorum;

  public OFlowSimulator(int networkInitQuorum) {
    this.networkInitQuorum = networkInitQuorum;
  }

  @Override
  public void onStateChange(ODatabaseId dbId, ONodeId nodeId, ODatabaseState state) {}

  @Override
  public void update(ONodeStateStore newState) {}

  private class TestAction implements OCompleteAction {
    private boolean success = false;
    private boolean failure = false;
    private boolean complete = false;
    private Optional<OAcceptResult> result;
    private final OCoordinatedDistributedOps ops;

    private TestAction(OCoordinatedDistributedOps ops) {
      this.ops = ops;
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
      this.complete = true;
      this.result = result;
      this.ops.completeExecution(promise);
    }

    @Override
    public OResponseCollector newResponseCollector(
        OTransactionIdPromise promise, int quorum, Set<ONodeId> nodes) {
      return new OResponseCollectorImpl(this, promise, quorum, nodes);
    }
  }

  private record TestDistributedMessage(OOperationMessage message, OTransactionIdPromise promise)
      implements ODistributedMessage {

    @Override
    public OTransactionIdPromise getPromiseId() {
      return promise;
    }

    @Override
    public Optional<OAcceptResult> validate(OOperationContext ctx) {
      return message.validate(ctx, promise);
    }

    @Override
    public void apply(OOperationContext ctx) {
      message.apply(ctx, promise);
    }

    @Override
    public void cancel(OOperationContext ctx) {
      message.cancel(ctx, promise);
    }

    @Override
    public OOperationMessage getOp() {
      return message;
    }

    @Override
    public void recoordinate(OOperationContext ctx) {
      ctx.recoordinateOperation(promise, message);
    }
  }

  public class TestOperationContext implements OOperationContext {

    private OCoordinatedDistributedOps ops;

    public TestOperationContext(OCoordinatedDistributedOps ops) {
      this.ops = ops;
    }

    @Override
    public OCoordinatedDistributedOps getOps() {
      return ops;
    }

    @Override
    public void mergeNode(
        ONodeId node,
        ONodeStateNetwork state,
        ONodeStateNetwork original,
        OTransactionIdPromise promise) {
      ops.mergeNode(node, state, original, promise);
    }

    @Override
    public void establish(
        OGroupId groupId, Set<ONodeId> candidates, OTransactionIdPromise promise) {
      ops.establish(groupId, candidates, promise);
    }

    @Override
    public void distributedDrop(ODatabaseId dbId, OVersion version, OTransactionIdPromise promise) {
      ops.dropDatabase(dbId, version, promise);
    }

    @Override
    public void declareDatabase(
        OTransactionIdPromise promise,
        ODatabaseId id,
        String name,
        Set<OAddNodeInfo> partecipants,
        int minimumQuorum) {
      ops.declareDatabase(promise, id, name, partecipants, minimumQuorum);
    }

    @Override
    public void registerNode(ONodeId node, OVersion version, OTransactionIdPromise promise) {
      ops.registerNode(node, version, promise);
    }

    @Override
    public void recoordinateOperation(OTransactionIdPromise promise, OOperationMessage op) {
      // TODO Auto-generated method stub
    }
  }

  public Optional<OAcceptResult> execute(OOperationMessage message) {
    var nodeId = nodes.get(0);
    var node = contexts.get(nodeId);
    TestAction action = new TestAction(node.getOps());
    var start = node.getOps().start(action);
    assertTrue(start.isPresent());
    var promise = start.get().promise();
    Set<ONodeId> partecipatingNodes = start.get().nodes();
    var cm = new TestDistributedMessage(message, promise);
    return executeTwoPhase(cm, node, action, partecipatingNodes);
  }

  private Optional<OAcceptResult> executeTwoPhase(
      ODistributedMessage message,
      TestOperationContext coordinator,
      TestAction action,
      Set<ONodeId> partecipatingNodes) {

    // First Phase
    List<ORawPair<ONodeId, Optional<OAcceptResult>>> results = new ArrayList<>();
    for (var nodeTo : partecipatingNodes) {
      var context = contexts.get(nodeTo);
      Optional<OAcceptResult> result = context.getOps().receive(message);
      if (result.isEmpty()) {
        Optional<OAcceptResult> res = message.validate(context);
        if (res.isPresent()) {
          // This is canceling the promise right away because is not accepted by the data
          context.getOps().cancelPromise(message.getPromiseId());
          results.add(new ORawPair<>(nodeTo, Optional.empty()));
        } else {
          results.add(new ORawPair<>(nodeTo, res));
        }
      } else {
        results.add(new ORawPair<>(nodeTo, result));
      }
    }
    // First Phase report responses to coordinator.
    for (var result : results) {
      if (result.second.isEmpty()) {
        coordinator.getOps().nodeSuccess(result.first, message.getPromiseId());
      } else {
        coordinator.getOps().nodeFailure(result.first, message.getPromiseId(), result.second.get());
      }
    }

    // Second Phase
    // The action is filled with the state computed from the results reported, by the coordinator.
    if (action.success) {
      for (var nodeTo : partecipatingNodes) {
        var context = contexts.get(nodeTo);
        var promisedMessage = context.getOps().consensusSuccess(message.getPromiseId());
        if (promisedMessage.isPresent()) {
          promisedMessage.get().apply(context);
        } else {
          fail("promised message not present");
        }
        context.getOps().completeExecution(message.getPromiseId());
      }
      assertTrue(action.complete);
      return Optional.empty();
    } else if (action.failure) {
      for (var nodeTo : partecipatingNodes) {
        var context = contexts.get(nodeTo);
        var promisedMessage = context.getOps().consensusFailure(message.getPromiseId());
        if (promisedMessage.isPresent()) {
          message.cancel(context);
        } else {
          // This should be ok .... it means it didn't promise it
        }
      }
      return action.result;
    }

    fail("no success, neither failure ... strange");
    return null;
  }

  private ONodeId newRandomNodeId() {
    return new ONodeId(UUID.randomUUID().toString());
  }

  private ONodeStateNetwork networkState(OTopologyStateNetwork topology) {
    return new ONodeStateNetwork(
        topology, Collections.emptyList(), new OTransactionSequenceStatus(new long[] {0, 0, 0}));
  }

  private ONodeStateNetwork bootNetworkState() {
    return networkState(OTopologyStateNetwork.boot(group));
  }

  public ONodeId bootNode() {
    var nodeId = newRandomNodeId();
    var ops = new OCoordinatedDistributedOpsImpl(nodeId, group, networkInitQuorum, this, this);
    var action = ops.nodeJoinStart(nodeId, bootNetworkState(), false);
    if (action instanceof ODiscoverAction.OEstablishAction) {
      // I'm alone and quorum 1 I create a network with only myself.
      Optional<OTransactionIdPromise> seq = ops.startEstablish(Set.of(nodeId), new TestAction(ops));
      ops.consensusSuccess(seq.get());
      ops.validateEstablish(group, Set.of(nodeId), seq.get());
      ops.establish(group, Set.of(nodeId), seq.get());
      ops.completeExecution(seq.get());
    }
    var networkState = ops.getNetworkState();
    nodes.add(nodeId);
    contexts.put(nodeId, new TestOperationContext(ops));
    for (var context : contexts.values()) {
      var joinAction = context.getOps().nodeJoinStart(nodeId, networkState, false);
      if (joinAction instanceof ODiscoverAction.OEstablishAction) {
        // Enough nodes to reach the required quorum create network.
        var candidates = ((ODiscoverAction.OEstablishAction) joinAction).candidates();
        var testAction = new TestAction(context.getOps());
        var enstablish = new OEstablishTopology(group, candidates);
        var seq = context.getOps().startEstablish(candidates, testAction);
        var cm = new TestDistributedMessage(enstablish, seq.get());
        executeTwoPhase(cm, context, testAction, candidates);
      } else if (joinAction instanceof ODiscoverAction.OAddNodeAction) {
        var toAdd = ((ODiscoverAction.OAddNodeAction) joinAction).node();
        var version = context.getOps().nextTopologyVersion();
        TestAction addAction = new TestAction(context.getOps());
        var start = context.getOps().start(addAction);
        assertTrue(start.isPresent());
        var promise = start.get().promise();
        Set<ONodeId> partecipatingNodes = start.get().nodes();
        var cm = new TestDistributedMessage(new OAddTopologyMember(version, toAdd), promise);
        var result = executeTwoPhase(cm, context, addAction, partecipatingNodes);
        if (result.isEmpty()) {
          var addedContext = contexts.get(nodeId);
          addedContext
              .getOps()
              .nodeJoinStart(
                  context.getOps().getCurrent(), context.getOps().getNetworkState(), false);
        }
      }
    }
    return nodeId;
  }

  public Map<ONodeId, TestOperationContext> getContexts() {
    return contexts;
  }
}
