package com.orientechnologies.orient.server.distributed.impl.coordinator;

public interface ODistributedChannel {
  void sendRequest(OLogId id, ONodeRequest nodeRequest);

  void sendResponse(OLogId id, ONodeResponse nodeResponse);

  void reply(OSubmitResponse response);
}
