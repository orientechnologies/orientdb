package com.orientechnologies.orient.server.distributed.impl.coordinator;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class ORequestContext {

  public enum Status {
    STARTED, QUORUM_OK, QUORUM_KO
  }

  private OSubmitRequest          submitRequest;
  private ONodeRequest            nodeRequest;
  private List<ONodeResponse>     responses = Collections.synchronizedList(new ArrayList<>());
  private ODistributedCoordinator coordinator;
  private int                     quorum;
  private OResponseHandler        handler;
  private Status                  status;

  public ORequestContext(ODistributedCoordinator coordinator, OSubmitRequest submitRequest, ONodeRequest nodeRequest, int quorum,
      OResponseHandler handler) {
    this.coordinator = coordinator;
    this.submitRequest = submitRequest;
    this.nodeRequest = nodeRequest;
    this.quorum = quorum;
    this.handler = handler;
    this.status = Status.STARTED;
  }

  public void receive(ONodeResponse response) {
    responses.add(response);
    status = handler.receive(coordinator, this, response, status);
  }

  public List<ONodeResponse> getResponses() {
    return responses;
  }

  public int getQuorum() {
    return quorum;
  }
}
