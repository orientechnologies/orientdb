package com.orientechnologies.orient.distributed.context;

import com.orientechnologies.orient.core.transaction.ONodeId;
import com.orientechnologies.orient.core.transaction.OTransactionIdPromise;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

public class OResponseCollector {

  public record CompleteInfo(
      OCompleteAction action, OTransactionIdPromise promise, Set<ONodeId> nodes) {}
  ;

  private final OCompleteAction action;
  private final int quorum;
  private final OTransactionIdPromise promise;
  private final Set<ONodeId> expected;
  private final Set<ONodeId> toReceive;
  private final Set<ONodeId> success = new HashSet<>();
  private final Set<ONodeId> lost = new HashSet<>();
  private final Set<ONodeId> failure = new HashSet<>();

  public OResponseCollector(
      OCompleteAction action, OTransactionIdPromise promise, int quorum, Set<ONodeId> activeNodes) {
    this.action = action;
    this.promise = promise;
    this.quorum = quorum;
    this.expected = activeNodes;
    this.toReceive = new HashSet<>(activeNodes);
  }

  public Optional<CompleteInfo> receive(ONodeId node) {
    if (toReceive.remove(node)) {
      success.add(node);
      if (success.size() == quorum) {
        return Optional.of(new CompleteInfo(action, promise, expected));
      }
    }
    return Optional.empty();
  }

  public boolean hasSuccessQuorum() {
    return success.size() >= quorum;
  }

  public boolean hasFailureQuorum() {
    return lost.size() + failure.size() >= quorum;
  }

  public boolean isFinished() {
    return expected.size() == success.size() + lost.size() + failure.size();
  }

  public boolean isFinishedNotQuorum() {
    return isFinished() && !hasFailureQuorum() && !hasSuccessQuorum();
  }

  public Set<ONodeId> getExpected() {
    return expected;
  }

  public Optional<CompleteInfo> disconnected(ONodeId node) {
    if (toReceive.remove(node)) {
      lost.add(node);
      if (lost.size() + failure.size() == quorum) {
        return Optional.of(new CompleteInfo(action, promise, expected));
      }
      if (isFinishedNotQuorum()) {
        return Optional.of(new CompleteInfo(action, promise, expected));
      }
    }
    return Optional.empty();
  }

  public Optional<CompleteInfo> fail(ONodeId node) {
    if (toReceive.remove(node)) {
      failure.add(node);
      if (lost.size() + failure.size() == quorum) {
        return Optional.of(new CompleteInfo(action, promise, expected));
      }
      if (isFinishedNotQuorum()) {
        return Optional.of(new CompleteInfo(action, promise, expected));
      }
    }
    return Optional.empty();
  }
}
