package com.orientechnologies.orient.server.distributed.impl.coordinator;

public interface ONodeRequest {
  ONodeResponse execute(ODistributedMember nodeFrom, OLogId opId, ODistributedExecutor executor);
}
