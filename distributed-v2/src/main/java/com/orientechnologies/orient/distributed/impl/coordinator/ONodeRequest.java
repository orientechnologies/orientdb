package com.orientechnologies.orient.distributed.impl.coordinator;

import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.config.ONodeIdentity;
import com.orientechnologies.orient.distributed.impl.log.OLogId;
import com.orientechnologies.orient.distributed.impl.log.OLogRequest;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public interface ONodeRequest extends OLogRequest {
  ONodeResponse execute(
      ONodeIdentity nodeFrom,
      OLogId opId,
      ODistributedExecutor executor,
      ODatabaseDocumentInternal session);

  default ONodeResponse recover(
      ONodeIdentity nodeFrom,
      OLogId opId,
      ODistributedExecutor executor,
      ODatabaseDocumentInternal session) {
    return execute(nodeFrom, opId, executor, session);
  }

  void serialize(DataOutput output) throws IOException;

  void deserialize(DataInput input) throws IOException;

  int getRequestType();
}
