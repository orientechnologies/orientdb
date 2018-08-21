package com.orientechnologies.orient.server.distributed.impl.coordinator;

import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;

public interface ONodeRequest {
  ONodeResponse execute(ODistributedMember nodeFrom, OLogId opId, ODistributedExecutor executor, ODatabaseDocumentInternal session);
}
