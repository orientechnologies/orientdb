package com.orientechnologies.orient.distributed.impl;

import java.util.HashSet;
import java.util.Set;

public class OLeaderElectionStateMachine {

  public enum Status {
    LEADER, FOLLOWER, CANDIDATE;
  }

  String      nodeName;
  int         currentTerm;
  Status      status;
  int         quorum;
  Set<String> votesReceived = new HashSet<>();
  int         lastTermVoted = -1;

  synchronized void receiveVote(int term, String fromNode, String toNode) {
    if (!nodeName.equals(toNode)) {
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
    votesReceived.add(nodeName);
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
