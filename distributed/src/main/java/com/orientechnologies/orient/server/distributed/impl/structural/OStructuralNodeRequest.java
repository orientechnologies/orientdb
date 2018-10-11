package com.orientechnologies.orient.server.distributed.impl.structural;

import com.orientechnologies.orient.core.db.OrientDBInternal;
import com.orientechnologies.orient.server.distributed.impl.coordinator.OLogId;
import com.orientechnologies.orient.server.distributed.impl.coordinator.OLogRequest;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public interface OStructuralNodeRequest extends OLogRequest {
  OStructuralNodeResponse execute(OStructuralDistributedMember nodeFrom, OLogId opId, OStructuralDistributedExecutor executor,
      OrientDBInternal context);

  void serialize(DataOutput output) throws IOException;

  void deserialize(DataInput input) throws IOException;

  int getRequestType();
}
