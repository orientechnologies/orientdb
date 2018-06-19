package com.orientechnologies.orient.server.distributed.impl.coordinator;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class ORequestContext {
  private OSubmitRequest          submitRequest;
  private ONodeRequest            nodeRequest;
  private List<ONodeResponse>     responses = Collections.synchronizedList(new ArrayList<>());
  private ODistributedCoordinator coordinator;
  private int                     quorum;
  private OResponseHandler        handler;

  public ORequestContext(ODistributedCoordinator coordinator, OSubmitRequest submitRequest, ONodeRequest nodeRequest, int quorum,
      OResponseHandler handler) {
    this.coordinator = coordinator;
    this.submitRequest = submitRequest;
    this.nodeRequest = nodeRequest;
    this.quorum = quorum;
    this.handler = handler;
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
