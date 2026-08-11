package com.orientechnologies.orient.distributed.context.coordination;

import com.orientechnologies.orient.core.id.ONodeId;
import com.orientechnologies.orient.distributed.context.coordination.topology.OTopologyState;
import java.util.Optional;
import java.util.Set;

public interface ONetworkTopology {
  OTopologyState getState();

  Set<ONodeId> getMembers();

  int getQuorum();

  boolean isSelfEnstablished();

  OVersion getVersion();

  Optional<ONodeId> getNodeId(String nodeName);
}
