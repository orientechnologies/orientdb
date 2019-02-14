package com.orientechnologies.orient.distributed.impl;

import com.orientechnologies.orient.core.db.config.ONodeIdentity;

import java.util.HashSet;
import java.util.Set;

public class OLeaderElectionStateMachine {

  public enum Status {
    LEADER, FOLLOWER, CANDIDATE;
  }

  ONodeIdentity      nodeIdentity;
  int                currentTerm;
  Status             status;
  int                quorum;
  Set<ONodeIdentity> votesReceived = new HashSet<>();
  int                lastTermVoted = -1;

  synchronized void receiveVote(int term, ONodeIdentity fromNode, ONodeIdentity toNode) {
    if (!nodeIdentity.equals(toNode)) {
      return;
    }
    if (currentTerm == term) {
      votesReceived.add(fromNode);
      if (votesReceived.size() >= quorum) {
        status = Status.LEADER;
      }
    } else if (currentTerm < term) {
      changeTerm(term);
    }
  }

  synchronized void startElection() {
    status = Status.CANDIDATE;
    currentTerm++;
    votesReceived.clear();
    votesReceived.add(nodeIdentity);
  }

  synchronized void changeTerm(int term) {
    status = Status.FOLLOWER;
    this.votesReceived.clear();
    this.currentTerm = term;
  }

  public void resetLeaderElection() {
    status = Status.FOLLOWER;
    this.votesReceived.clear();
  }

  public Status getStatus() {
    return status;
  }

  public void setStatus(Status status) {
    this.status = status;
  }

  public void setCurrentTerm(int currentTerm) {
    this.currentTerm = currentTerm;
  }

  public void setQuorum(int quorum) {
    this.quorum = quorum;
  }

}
