package com.orientechnologies.orient.distributed.context.coordination;

import com.orientechnologies.orient.core.transaction.ONodeId;
import com.orientechnologies.orient.distributed.context.coordination.topology.OTopologyState;
import java.util.Set;

public interface ONetworkTopology {
  OTopologyState getState();

  Set<ONodeId> getMembers();

  int getQuorum();

  boolean isSelfEnstablished();
}
