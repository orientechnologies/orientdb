package com.orientechnologies.orient.server.distributed;

public interface ORemoteServerAvailabilityCheck {

  boolean isNodeAvailable(String node);

  void nodeDisconnected(String node);
}
