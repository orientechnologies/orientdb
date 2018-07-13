package com.orientechnologies.orient.server.distributed.impl.coordinator;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

public class ORequestContext {

  public enum Status {
    STARTED, QUORUM_OK, QUORUM_KO
  }

  private OSubmitRequest                 submitRequest;
  private ONodeRequest                   nodeRequest;
  private Collection<ODistributedMember> involvedMembers;
  private List<ONodeResponse>            responses = Collections.synchronizedList(new ArrayList<>());
  private ODistributedCoordinator        coordinator;
  private int                            quorum;
  private OResponseHandler               handler;

  public ORequestContext(ODistributedCoordinator coordinator, OSubmitRequest submitRequest, ONodeRequest nodeRequest,
      Collection<ODistributedMember> involvedMembers, OResponseHandler handler) {
    this.coordinator = coordinator;
    this.submitRequest = submitRequest;
    this.nodeRequest = nodeRequest;
    this.involvedMembers = involvedMembers;
    this.handler = handler;
    this.quorum = (involvedMembers.size() / 2) + 1;
  }

  public void receive(ONodeResponse response) {
    responses.add(response);
    handler.receive(coordinator, this, response);
  }

  public List<ONodeResponse> getResponses() {
    return responses;
  }

  public int getQuorum() {
    return quorum;
  }
}
