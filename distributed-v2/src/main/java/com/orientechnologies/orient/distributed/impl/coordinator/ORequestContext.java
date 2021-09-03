package com.orientechnologies.orient.distributed.impl.coordinator;

import com.orientechnologies.orient.core.db.config.ONodeIdentity;
import com.orientechnologies.orient.distributed.impl.log.OLogId;
import java.util.Collection;
import java.util.Map;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;

public class ORequestContext {

  public enum Status {
    STARTED,
    QUORUM_OK,
    QUORUM_KO
  }

  private OSubmitRequest submitRequest;
  private ONodeRequest nodeRequest;
  private Collection<ONodeIdentity> involvedMembers;
  private Map<ONodeIdentity, ONodeResponse> responses = new ConcurrentHashMap<>();
  private ODistributedCoordinator coordinator;
  private int quorum;
  private OResponseHandler handler;
  private TimerTask timerTask;
  private OLogId requestId;

  public ORequestContext(
      ODistributedCoordinator coordinator,
      OSubmitRequest submitRequest,
      ONodeRequest nodeRequest,
      Collection<ONodeIdentity> involvedMembers,
      OResponseHandler handler,
      OLogId requestId) {
    this.coordinator = coordinator;
    this.submitRequest = submitRequest;
    this.nodeRequest = nodeRequest;
    this.involvedMembers = involvedMembers;
    this.handler = handler;
    this.quorum = (involvedMembers.size() / 2) + 1;
    this.requestId = requestId;

    timerTask =
        new TimerTask() {
          @Override
          public void run() {
            coordinator.executeOperation(
                () -> {
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

  public void receive(ONodeIdentity member, ONodeResponse response) {
    responses.put(member, response);
    if (handler.receive(coordinator, this, member, response)) {
      timerTask.cancel();
      finish();
    }
  }

  public Map<ONodeIdentity, ONodeResponse> getResponses() {
    return responses;
  }

  public int getQuorum() {
    return quorum;
  }

  public TimerTask getTimerTask() {
    return timerTask;
  }

  public Collection<ONodeIdentity> getInvolvedMembers() {
    return involvedMembers;
  }
}
