package com.orientechnologies.orient.server.distributed.impl.coordinator;

public interface ONodeRequest extends ONodeMessage {
  ONodeResponse execute(String nodeFrom, OLogId opId, ODistributedExecutor executor);
}
