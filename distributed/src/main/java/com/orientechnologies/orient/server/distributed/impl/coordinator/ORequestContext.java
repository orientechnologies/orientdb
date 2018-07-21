package com.orientechnologies.orient.server.distributed.impl.coordinator;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class ORequestContext {

  public enum Status {
    STARTED, QUORUM_OK, QUORUM_KO
  }

  private OSubmitRequest                         submitRequest;
  private ONodeRequest                           nodeRequest;
  private Collection<ODistributedMember>         involvedMembers;
  private Map<ODistributedMember, ONodeResponse> responses = new ConcurrentHashMap<>();
  private ODistributedCoordinator                coordinator;
  private int                                    quorum;
  private OResponseHandler                       handler;
  private TimerTask                              timerTask;

  public ORequestContext(ODistributedCoordinator coordinator, OSubmitRequest submitRequest, ONodeRequest nodeRequest,
      Collection<ODistributedMember> involvedMembers, OResponseHandler handler) {
    this.coordinator = coordinator;
    this.submitRequest = submitRequest;
    this.nodeRequest = nodeRequest;
    this.involvedMembers = involvedMembers;
    this.handler = handler;
    this.quorum = (involvedMembers.size() / 2) + 1;

    timerTask = new TimerTask() {
      @Override
      public void run() {
        coordinator.executeOperation(() -> {
          handler.timeout(coordinator, ORequestContext.this);
          //TODO:Cancel the task depending on the timeout action.
        });
      }
    };

  }

  public void receive(ODistributedMember member, ONodeResponse response) {
    responses.put(member, response);
    handler.receive(coordinator, this, member, response);
  }

  public Map<ODistributedMember, ONodeResponse> getResponses() {
    return responses;
  }

  public int getQuorum() {
    return quorum;
  }

  public TimerTask getTimerTask() {
    return timerTask;
  }
}
