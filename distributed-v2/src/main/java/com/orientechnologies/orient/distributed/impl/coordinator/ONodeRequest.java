package com.orientechnologies.orient.distributed.impl.coordinator;

import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public interface ONodeRequest extends OLogRequest {
  ONodeResponse execute(ODistributedMember nodeFrom, OLogId opId, ODistributedExecutor executor, ODatabaseDocumentInternal session);

  default ONodeResponse recover(ODistributedMember nodeFrom, OLogId opId, ODistributedExecutor executor,
      ODatabaseDocumentInternal session) {
    return execute(nodeFrom, opId, executor, session);
  }

  void serialize(DataOutput output) throws IOException;

  void deserialize(DataInput input) throws IOException;

  int getRequestType();
}
