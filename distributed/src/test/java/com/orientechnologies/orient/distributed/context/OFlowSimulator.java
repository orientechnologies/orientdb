package com.orientechnologies.orient.distributed.context;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.orientechnologies.common.util.ORawPair;
import com.orientechnologies.orient.core.transaction.ODatabaseId;
import com.orientechnologies.orient.core.transaction.OGroupId;
import com.orientechnologies.orient.core.transaction.ONodeId;
import com.orientechnologies.orient.core.transaction.OTransactionIdPromise;
import com.orientechnologies.orient.core.tx.OTransactionSequenceStatus;
import com.orientechnologies.orient.distributed.context.coordination.OConsensusSuccess;
import com.orientechnologies.orient.distributed.context.coordination.OCoordinatedDistributedOpsImpl;
import com.orientechnologies.orient.distributed.context.coordination.dbs.ODatabaseState;
import com.orientechnologies.orient.distributed.context.coordination.dbs.ODatabaseStateChangeListener;
import com.orientechnologies.orient.distributed.context.coordination.message.ODistributedMessage;
import com.orientechnologies.orient.distributed.context.coordination.message.operation.OAddTopologyMember;
import com.orientechnologies.orient.distributed.context.coordination.message.operation.OEstablishTopology;
import com.orientechnologies.orient.distributed.context.coordination.message.operation.OOperationMessage;
import com.orientechnologies.orient.distributed.context.coordination.message.state.ONodeStateNetwork;
import com.orientechnologies.orient.distributed.context.coordination.message.state.OTopologyStateNetwork;
import com.orientechnologies.orient.distributed.context.coordination.result.OAcceptResult;
import com.orientechnologies.orient.distributed.context.coordination.topology.ODiscoverAction;
import com.orientechnologies.orient.distributed.context.simulator.TestAction;
import com.orientechnologies.orient.distributed.context.simulator.TestDistributedMessage;
import com.orientechnologies.orient.distributed.context.simulator.TestOperationContext;
import java.util.ArrayList;
import java.util.Collection;
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
  private int nodeSeq = 0;

  public OFlowSimulator(int networkInitQuorum) {
    this.networkInitQuorum = networkInitQuorum;
  }

  @Override
  public void onStateChange(ODatabaseId dbId, ONodeId nodeId, ODatabaseState state) {}

  @Override
  public void update(ONodeStateStore newState) {}

  /** Run to message concurrently the second will be the one failing because of the order of execution.
   *
   * @param message
   * @param messageSecond
   * @return
   */
  public void executeConcurrently(OOperationMessage message, OOperationMessage messageSecond) {
    var nodeId = nodes.get(0);
    var node = contexts.get(nodeId);
    TestAction action = new TestAction(node.getOps());
    var start = node.getOps().start(action);
    assertTrue(start.isPresent());
    var promise = start.get().promise();
    Set<ONodeId> partecipatingNodes = start.get().nodes();
    var cm = new TestDistributedMessage(message, promise);

    var nodeIdSecond = nodes.get(1);
    var nodeSecond = contexts.get(nodeIdSecond);
    TestAction actionSecond = new TestAction(nodeSecond.getOps());
    var startSecond = nodeSecond.getOps().start(actionSecond);
    assertTrue(startSecond.isPresent());
    var promiseSecond = startSecond.get().promise();
    var cmSecond = new TestDistributedMessage(messageSecond, promiseSecond);
    executeConcurrentTwoPhase(
        cm, cmSecond, node, nodeSecond, action, actionSecond, partecipatingNodes);
  }

  /** Run to message concurrently the second will be the one failing because of the order of execution.
   *
   * @param message
   * @param messageSecond
   * @return
   */
  public void executeConcurrentlySecondConfirmFirst(
      OOperationMessage message, OOperationMessage messageSecond) {
    var nodeId = nodes.get(0);
    var node = contexts.get(nodeId);
    TestAction action = new TestAction(node.getOps());
    var start = node.getOps().start(action);
    assertTrue(start.isPresent());
    var promise = start.get().promise();
    Set<ONodeId> partecipatingNodes = start.get().nodes();
    var cm = new TestDistributedMessage(message, promise);

    var nodeIdSecond = nodes.get(1);
    var nodeSecond = contexts.get(nodeIdSecond);
    TestAction actionSecond = new TestAction(nodeSecond.getOps());
    var startSecond = nodeSecond.getOps().start(actionSecond);
    assertTrue(startSecond.isPresent());
    var promiseSecond = startSecond.get().promise();
    var cmSecond = new TestDistributedMessage(messageSecond, promiseSecond);
    executeConcurrentTwoPhaseOtherConfirmFirst(
        cm, cmSecond, node, nodeSecond, action, actionSecond, partecipatingNodes);
  }

  private void executeConcurrentTwoPhaseOtherConfirmFirst(
      ODistributedMessage message,
      ODistributedMessage secondMessage,
      TestOperationContext coordinator,
      TestOperationContext secondCoordinator,
      TestAction action,
      TestAction secondAction,
      Set<ONodeId> partecipatingNodes) {

    var firstFlow = new ArrayList<>(partecipatingNodes);
    var secondFlow = new ArrayList<>(partecipatingNodes);
    Collections.reverse(firstFlow);

    // First Phase
    List<ORawPair<ONodeId, Optional<OAcceptResult>>> resultsFirst = new ArrayList<>();
    List<ORawPair<ONodeId, Optional<OAcceptResult>>> resultsSecond = new ArrayList<>();
    for (int i = 0; i < firstFlow.size(); i++) {
      var nodeToFirst = firstFlow.get(i);
      var context = contexts.get(nodeToFirst);
      var firstResult = validateMessage(message, context);
      resultsFirst.add(new ORawPair<>(nodeToFirst, firstResult));

      var nodeToSecond = secondFlow.get(i);
      var secondContext = contexts.get(nodeToSecond);
      var secondResult = validateMessage(secondMessage, secondContext);
      resultsSecond.add(new ORawPair<>(nodeToSecond, secondResult));
    }
    // First Phase report responses to coordinator.
    for (var result : resultsFirst) {
      validationResultToCoordinator(message, coordinator, result);
    }
    for (var result : resultsSecond) {
      validationResultToCoordinator(secondMessage, secondCoordinator, result);
    }

    // Second Phase
    // The action is filled with the state computed from the results reported, by the coordinator.
    secondPhaseExecution(secondMessage, secondAction, secondFlow);

    secondPhaseExecution(message, action, firstFlow);
  }

  protected void secondPhaseExecution(
      ODistributedMessage message, TestAction action, Collection<ONodeId> to) {
    if (action.isSuccess()) {
      executeSuccess(message, to);
      assertTrue(action.isComplete());
    } else if (action.isFailure()) {
      executeFailure(message, to);
    }
  }

  private void executeConcurrentTwoPhase(
      ODistributedMessage message,
      ODistributedMessage secondMessage,
      TestOperationContext coordinator,
      TestOperationContext secondCoordinator,
      TestAction action,
      TestAction secondAction,
      Set<ONodeId> partecipatingNodes) {

    var firstFlow = new ArrayList<>(partecipatingNodes);
    var secondFlow = new ArrayList<>(partecipatingNodes);
    Collections.reverse(firstFlow);

    // First Phase
    List<ORawPair<ONodeId, Optional<OAcceptResult>>> resultsFirst = new ArrayList<>();
    List<ORawPair<ONodeId, Optional<OAcceptResult>>> resultsSecond = new ArrayList<>();
    for (int i = 0; i < firstFlow.size(); i++) {
      var nodeToFirst = firstFlow.get(i);
      var context = contexts.get(nodeToFirst);
      var firstResult = validateMessage(message, context);
      resultsFirst.add(new ORawPair<>(nodeToFirst, firstResult));

      var nodeToSecond = secondFlow.get(i);
      var secondContext = contexts.get(nodeToSecond);
      var secondResult = validateMessage(secondMessage, secondContext);
      resultsSecond.add(new ORawPair<>(nodeToSecond, secondResult));
    }
    // First Phase report responses to coordinator.
    for (var result : resultsFirst) {
      validationResultToCoordinator(message, coordinator, result);
    }
    for (var result : resultsSecond) {
      validationResultToCoordinator(secondMessage, secondCoordinator, result);
    }

    secondPhaseExecution(message, action, firstFlow);

    secondPhaseExecution(secondMessage, secondAction, secondFlow);
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
      var result = validateMessage(message, context);
      results.add(new ORawPair<>(nodeTo, result));
    }
    // First Phase report responses to coordinator.
    for (var result : results) {
      validationResultToCoordinator(message, coordinator, result);
    }

    // Second Phase
    // The action is filled with the state computed from the results reported, by the coordinator.
    if (action.isSuccess()) {
      executeSuccess(message, partecipatingNodes);
      assertTrue(action.isComplete());
      return Optional.empty();
    } else if (action.isFailure()) {
      executeFailure(message, partecipatingNodes);
      return action.getResult();
    }

    fail("no success, neither failure ... strange");
    return null;
  }

  protected void executeFailure(
      ODistributedMessage message, Collection<ONodeId> partecipatingNodes) {
    for (var nodeTo : partecipatingNodes) {
      var context = contexts.get(nodeTo);
      var promisedMessage = context.getOps().consensusFailure(message.getPromiseId());
      if (promisedMessage.isPresent()) {
        message.cancel(context);
      } else {
        // This should be ok .... it means it didn't promise it
      }
    }
  }

  protected void executeSuccess(
      ODistributedMessage message, Collection<ONodeId> partecipatingNodes) {
    for (var nodeTo : partecipatingNodes) {
      var context = contexts.get(nodeTo);
      OConsensusSuccess successResult;
      do {
        successResult = context.getOps().consensusSuccess(message.getPromiseId());
        if (successResult.isFailure()) {
          var cancelled = context.getOps().consensusFailure(successResult.failure());
          if (cancelled.isPresent()) {
            message.cancel(context);
          }
        } else if (successResult.isSuccess()) {
          successResult.success().apply(context);
          context.getOps().completeExecution(message.getPromiseId());
        } else {
          fail("promised message not present");
        }
      } while (!successResult.isFinished());
    }
  }

  protected void validationResultToCoordinator(
      ODistributedMessage message,
      TestOperationContext coordinator,
      ORawPair<ONodeId, Optional<OAcceptResult>> result) {
    if (result.second.isEmpty()) {
      coordinator.getOps().nodeSuccess(result.first, message.getPromiseId());
    } else {
      coordinator.getOps().nodeFailure(result.first, message.getPromiseId(), result.second.get());
    }
  }

  protected Optional<OAcceptResult> validateMessage(
      ODistributedMessage message, TestOperationContext context) {
    Optional<OAcceptResult> result = context.getOps().receive(message);
    if (result.isEmpty()) {
      Optional<OAcceptResult> res = message.validate(context);
      if (res.isPresent()) {
        // This is canceling the promise right away because is not accepted by the data
        context.getOps().cancelPromise(message.getPromiseId());
      }
      return res;
    } else {
      return result;
    }
  }

  private ONodeId newNodeIdSeq() {

    return new ONodeId("node" + this.nodeSeq++);
  }

  private ONodeStateNetwork networkState(OTopologyStateNetwork topology) {
    return new ONodeStateNetwork(
        topology, Collections.emptyList(), new OTransactionSequenceStatus(new long[] {0, 0, 0}));
  }

  private ONodeStateNetwork bootNetworkState() {
    return networkState(OTopologyStateNetwork.boot(group));
  }

  public ONodeId bootNode() {
    var nodeId = newNodeIdSeq();
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
      var cOps = context.getOps();
      var joinAction = cOps.nodeJoinStart(nodeId, networkState, false);
      if (joinAction instanceof ODiscoverAction.OEstablishAction) {
        // Enough nodes to reach the required quorum create network.
        var candidates = ((ODiscoverAction.OEstablishAction) joinAction).candidates();
        var testAction = new TestAction(cOps);
        var enstablish = new OEstablishTopology(group, candidates);
        var seq = cOps.startEstablish(candidates, testAction);
        var cm = new TestDistributedMessage(enstablish, seq.get());
        executeTwoPhase(cm, context, testAction, candidates);
      } else if (joinAction instanceof ODiscoverAction.OAddNodeAction) {
        var toAdd = ((ODiscoverAction.OAddNodeAction) joinAction).node();
        var version = cOps.nextTopologyVersion();
        TestAction addAction = new TestAction(cOps);
        var start = cOps.start(addAction);
        assertTrue(start.isPresent());
        var promise = start.get().promise();
        Set<ONodeId> partecipatingNodes = start.get().nodes();
        var cm = new TestDistributedMessage(new OAddTopologyMember(version, toAdd), promise);
        var result = executeTwoPhase(cm, context, addAction, partecipatingNodes);
        if (result.isEmpty()) {
          var addedContextOps = contexts.get(nodeId).getOps();
          addedContextOps.nodeJoinStart(cOps.getCurrent(), cOps.getNetworkState(), false);
        }
      }
    }
    return nodeId;
  }

  public Map<ONodeId, TestOperationContext> getContexts() {
    return contexts;
  }
}
