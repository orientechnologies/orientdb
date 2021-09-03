package com.orientechnologies.orient.distributed.network;

import com.orientechnologies.orient.core.db.config.ONodeIdentity;
import java.util.HashSet;
import java.util.Set;

public class OLeaderElectionStateMachine {

  public enum Status {
    FOLLOWER,
    CANDIDATE,
    LEADER;
  }

  protected ONodeIdentity nodeIdentity;
  protected int currentTerm = -1;
  private volatile Status status = Status.FOLLOWER;
  protected int quorum;
  protected Set<ONodeIdentity> votesReceived = new HashSet<>();
  protected int lastTermVoted = -1;

  synchronized void receiveVote(int term, ONodeIdentity fromNode, ONodeIdentity toNode) {
    if (!nodeIdentity.equals(toNode)) {
      return;
    }
    if (currentTerm == term) {
      votesReceived.add(fromNode);
      if (votesReceived.size() >= quorum) {
        setStatus(Status.LEADER);
      }
    } else if (currentTerm < term) {
      changeTerm(term);
    }
  }

  synchronized void startElection() {
    setStatus(Status.CANDIDATE);
    currentTerm++;
    votesReceived.clear();
    votesReceived.add(nodeIdentity);
  }

  synchronized void changeTerm(int term) {
    if (term == this.currentTerm) {
      return;
    }
    setStatus(Status.FOLLOWER);
    this.votesReceived.clear();
    this.currentTerm = term;
  }

  public void resetLeaderElection() {
    setStatus(Status.FOLLOWER);
    this.votesReceived.clear();
  }

  public Status getStatus() {
    return status;
  }

  public void setStatus(Status status) {
    //    System.out.println(nodeIdentity.getName() + " setting status to " + status);
    this.status = status;
  }

  public void setCurrentTerm(int currentTerm) {
    this.currentTerm = currentTerm;
  }

  public void setQuorum(int quorum) {
    this.quorum = quorum;
  }
}
