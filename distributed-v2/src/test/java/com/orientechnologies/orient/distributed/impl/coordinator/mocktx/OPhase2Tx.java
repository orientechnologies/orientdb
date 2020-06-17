package com.orientechnologies.orient.distributed.impl.coordinator.mocktx;

import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.config.ONodeIdentity;
import com.orientechnologies.orient.distributed.impl.coordinator.ODistributedExecutor;
import com.orientechnologies.orient.distributed.impl.coordinator.ONodeRequest;
import com.orientechnologies.orient.distributed.impl.coordinator.ONodeResponse;
import com.orientechnologies.orient.distributed.impl.log.OLogId;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class OPhase2Tx implements ONodeRequest {
  @Override
  public ONodeResponse execute(
      ONodeIdentity nodeFrom,
      OLogId opId,
      ODistributedExecutor executor,
      ODatabaseDocumentInternal session) {
    return new OPhase2TxOk();
  }

  @Override
  public void serialize(DataOutput output) throws IOException {}

  @Override
  public void deserialize(DataInput input) throws IOException {}

  @Override
  public int getRequestType() {
    return 0;
  }
}
