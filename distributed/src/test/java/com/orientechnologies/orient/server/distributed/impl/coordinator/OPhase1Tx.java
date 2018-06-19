package com.orientechnologies.orient.server.distributed.impl.coordinator;

public class OPhase1Tx implements ONodeRequest {
  @Override
  public ONodeResponse execute(String nodeFrom, OLogId opId, ODistributedExecutor executor) {
    return new OPhase1TxOk();
  }
}
