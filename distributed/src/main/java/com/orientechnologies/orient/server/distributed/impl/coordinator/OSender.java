package com.orientechnologies.orient.server.distributed.impl.coordinator;

public interface OSender {

  void sendAll(ONodeMessage request);

  void sendTo(String node, ONodeMessage request);
}
