package com.orientechnologies.orient.server.distributed.impl.coordinator.mocktx;

import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.server.distributed.impl.coordinator.ODistributedExecutor;
import com.orientechnologies.orient.server.distributed.impl.coordinator.ODistributedMember;
import com.orientechnologies.orient.server.distributed.impl.coordinator.OLogId;
import com.orientechnologies.orient.server.distributed.impl.coordinator.ONodeRequest;
import com.orientechnologies.orient.server.distributed.impl.coordinator.ONodeResponse;

public class OPhase2Tx implements ONodeRequest {
  @Override
  public ONodeResponse execute(ODistributedMember nodeFrom, OLogId opId, ODistributedExecutor executor,
      ODatabaseDocumentInternal session) {
    return new OPhase2TxOk();
  }
}
