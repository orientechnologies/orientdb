package com.orientechnologies.orient.server.distributed;

import com.orientechnologies.orient.core.id.ONodeId;

public interface ORemoteServerAvailabilityCheck {

  boolean isNodeAvailable(ONodeId node);

  void nodeDisconnected(ONodeId node);
}
