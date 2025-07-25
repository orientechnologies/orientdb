package com.orientechnologies.orient.distributed.context.topology;

import com.orientechnologies.orient.core.transaction.ONodeId;

public interface OTopologyEvents {

  void nodeDiscovered(ONodeId node);
}
