package com.orientechnologies.orient.distributed.context.coordination.topology;

import com.orientechnologies.orient.core.transaction.ONodeId;

public interface OTopologyEvents {

  ODiscoverAction nodeDiscovered(ONodeId node);
}
