package com.orientechnologies.orient.distributed.context.coordination;

import com.orientechnologies.orient.core.transaction.ONodeId;
import com.orientechnologies.orient.core.transaction.OTransactionIdPromise;
import com.orientechnologies.orient.distributed.context.coordination.action.OCompleteAction;
import com.orientechnologies.orient.distributed.context.coordination.result.OAcceptResult;
import com.orientechnologies.orient.distributed.context.coordination.result.OQuorumNotReached;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public class OResponseCollectorMerge implements OResponseCollector {

  private final OCompleteAction action;
  private final int quorum;
  private final OTransactionIdPromise promise;
  private final Set<ONodeId> expected;
  private final Set<ONodeId> toReceive;
  private final Set<ONodeId> success = new HashSet<>();
  private final Set<ONodeId> lost = new HashSet<>();
  private final Set<ONodeId> failure = new HashSet<>();
  private final Map<OAcceptResult, Integer> results = new HashMap<>();
  private boolean applied = false;
  private ONodeId mergeNode;
  private boolean mergeResponse;
  private boolean mergeSuccess;
  private Optional<OAcceptResult> mergeFailure = Optional.empty();

  public OResponseCollectorMerge(
      OCompleteAction action,
      OTransactionIdPromise promise,
      int quorum,
      Set<ONodeId> nodes,
      ONodeId mergeNode) {
    assert quorum > 0;
    this.action = action;
    this.promise = promise;
    this.quorum = quorum;
    this.expected = nodes;
    this.toReceive = new HashSet<>(nodes);
    this.mergeNode = mergeNode;
  }

  public Optional<CompleteInfo> receive(ONodeId node) {
    if (node.equals(this.mergeNode)) {
      mergeSuccess = true;
      mergeResponse = true;
    } else if (toReceive.remove(node)) {
      success.add(node);
    }
    if (success.size() == quorum && mergeSuccess) {
      return Optional.of(new CompleteInfo(action, promise, expected, Optional.empty()));
    }
    return Optional.empty();
  }

  public boolean hasSuccessQuorum() {
    return success.size() >= quorum && mergeSuccess;
  }

  public boolean hasFailureQuorum() {
    return lost.size() + failure.size() >= quorum || mergeFailure.isPresent();
  }

  public boolean isFinished() {
    return expected.size() == success.size() + lost.size() + failure.size() && mergeResponse;
  }

  public boolean isTotallyFinished() {
    if (hasSuccessQuorum()) {
      return isFinished() && applied;
    } else {
      return isFinished();
    }
  }

  public boolean isFinishedNotQuorum() {
    return isFinished() && !hasFailureQuorum() && !hasSuccessQuorum();
  }

  public Set<ONodeId> getExpected() {
    return expected;
  }

  public Optional<CompleteInfo> disconnected(ONodeId node) {
    if (node.equals(this.mergeNode)) {
      mergeFailure = Optional.of(new OQuorumNotReached(Collections.emptySet()));
      mergeResponse = true;
    } else if (toReceive.remove(node)) {
      lost.add(node);
      if (lost.size() + failure.size() == quorum) {
        OAcceptResult result = computeResult();
        return Optional.of(new CompleteInfo(action, promise, expected, Optional.of(result)));
      }
      if (isFinishedNotQuorum()) {
        OAcceptResult result = computeResult();
        return Optional.of(new CompleteInfo(action, promise, expected, Optional.of(result)));
      }
    }
    return Optional.empty();
  }

  public Optional<CompleteInfo> fail(ONodeId node, OAcceptResult acceptResult) {
    if (node.equals(this.mergeNode)) {
      mergeFailure = Optional.of(acceptResult);
      mergeResponse = true;
    } else if (toReceive.remove(node)) {
      failure.add(node);
      this.results.compute(acceptResult, this::defaultResult);
    }
    if (lost.size() + failure.size() == quorum || mergeFailure.isPresent()) {
      OAcceptResult result = computeResult();
      return Optional.of(new CompleteInfo(action, promise, expected, Optional.of(result)));
    }
    if (isFinishedNotQuorum()) {
      OAcceptResult result = computeResult();
      return Optional.of(new CompleteInfo(action, promise, expected, Optional.of(result)));
    }
    return Optional.empty();
  }

  private Integer defaultResult(OAcceptResult res, Integer v) {
    if (v == null) {
      return 1;
    } else {
      return v + 1;
    }
  }

  private OAcceptResult computeResult() {
    if (mergeFailure.isPresent()) {
      return mergeFailure.get();
    }
    for (var res : this.results.entrySet()) {
      if (res.getValue() >= quorum) {
        return res.getKey();
      }
    }
    return new OQuorumNotReached(this.results.keySet());
  }

  public Optional<CompleteInfo> applied() {
    applied = true;
    return Optional.of(new CompleteInfo(action, promise, expected, Optional.empty()));
  }
}
