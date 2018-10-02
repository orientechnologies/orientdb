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
  private ODistributedCoordinatorInternal        coordinator;
  private int                                    quorum;
  private OResponseHandler                       handler;
  private TimerTask                              timerTask;
  private OLogId                                 requestId;

  public ORequestContext(ODistributedCoordinatorInternal coordinator, OSubmitRequest submitRequest, ONodeRequest nodeRequest,
      Collection<ODistributedMember> involvedMembers, OResponseHandler handler, OLogId requestId) {
    this.coordinator = coordinator;
    this.submitRequest = submitRequest;
    this.nodeRequest = nodeRequest;
    this.involvedMembers = involvedMembers;
    this.handler = handler;
    this.quorum = (involvedMembers.size() / 2) + 1;
    this.requestId = requestId;

    timerTask = new TimerTask() {
      @Override
      public void run() {
        coordinator.executeOperation(() -> {
          if (handler.timeout(coordinator, ORequestContext.this)) {
            finish();
          }
        });
      }
    };

  }

  public void finish() {
    coordinator.finish(requestId);
  }

  public void receive(ODistributedMember member, ONodeResponse response) {
    responses.put(member, response);
    if (handler.receive(coordinator, this, member, response)) {
      timerTask.cancel();
      finish();
    }
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

  public Collection<ODistributedMember> getInvolvedMembers() {
    return involvedMembers;
  }
}
