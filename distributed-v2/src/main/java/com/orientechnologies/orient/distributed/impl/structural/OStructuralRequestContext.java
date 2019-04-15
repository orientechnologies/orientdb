package com.orientechnologies.orient.distributed.impl.structural;

import com.orientechnologies.orient.distributed.impl.coordinator.OLogId;

import java.util.Collection;
import java.util.Map;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;

public class OStructuralRequestContext {

  public enum Status {
    STARTED, QUORUM_OK, QUORUM_KO
  }

  private OStructuralNodeRequest                                     nodeRequest;
  private Collection<OStructuralDistributedMember>                   involvedMembers;
  private Map<OStructuralDistributedMember, OStructuralNodeResponse> responses = new ConcurrentHashMap<>();
  private OStructuralCoordinator                                     coordinator;
  private int                                                        quorum;
  private OStructuralResponseHandler                                 handler;
  private TimerTask                                                  timerTask;
  private OLogId                                                     requestId;

  public OStructuralRequestContext(OStructuralCoordinator coordinator, OStructuralNodeRequest nodeRequest, Collection<OStructuralDistributedMember> involvedMembers,
      OStructuralResponseHandler handler, OLogId requestId) {
    this.coordinator = coordinator;
    this.nodeRequest = nodeRequest;
    this.involvedMembers = involvedMembers;
    this.handler = handler;
    this.quorum = (involvedMembers.size() / 2) + 1;
    this.requestId = requestId;

    timerTask = new TimerTask() {
      @Override
      public void run() {
        coordinator.executeOperation(() -> {
          if (handler.timeout(coordinator, OStructuralRequestContext.this)) {
            finish();
          }
        });
      }
    };

  }

  public void finish() {
    coordinator.finish(requestId);
  }

  public void receive(OStructuralDistributedMember member, OStructuralNodeResponse response) {
    responses.put(member, response);
    if (handler.receive(coordinator, this, member, response)) {
      timerTask.cancel();
      finish();
    }
  }

  public Map<OStructuralDistributedMember, OStructuralNodeResponse> getResponses() {
    return responses;
  }

  public int getQuorum() {
    return quorum;
  }

  public TimerTask getTimerTask() {
    return timerTask;
  }

  public Collection<OStructuralDistributedMember> getInvolvedMembers() {
    return involvedMembers;
  }
}
