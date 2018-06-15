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

  public ORequestContext(ODistributedCoordinator coordinator, OSubmitRequest submitRequest, ONodeRequest nodeRequest) {
    this.coordinator = coordinator;
    this.submitRequest = submitRequest;
    this.nodeRequest = nodeRequest;
  }

  public void receive(ONodeResponse response) {
    responses.add(response);
    if (responses.size() > quorum) {
      //TODO Trigger the second phase scheduling a task in the executor
      //this.coordinator.
    }
  }
}
