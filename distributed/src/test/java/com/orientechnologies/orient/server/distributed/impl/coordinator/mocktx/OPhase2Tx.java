package com.orientechnologies.orient.server.distributed.impl.coordinator.mocktx;

import com.orientechnologies.orient.server.distributed.impl.coordinator.*;

public class OPhase2Tx implements ONodeRequest {
  @Override
  public ONodeResponse execute(ODistributedMember nodeFrom, OLogId opId, ODistributedExecutor executor) {
    return new OPhase2TxOk();
  }
}
