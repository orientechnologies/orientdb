package com.orientechnologies.orient.server.distributed.impl.coordinator.transaction;

import com.orientechnologies.orient.server.distributed.impl.coordinator.ONodeResponse;

public class OTransactionSecondPhaseResponse implements ONodeResponse {
  private boolean success;

  public OTransactionSecondPhaseResponse(boolean success) {
    this.success = success;
  }
}
