package com.orientechnologies.orient.server.distributed.impl.coordinator;

public interface OSender {

  void sendTo(String node, OLogId id, ONodeMessage request);

  void sendResponse(String node, OSubmitResponse response);
}
