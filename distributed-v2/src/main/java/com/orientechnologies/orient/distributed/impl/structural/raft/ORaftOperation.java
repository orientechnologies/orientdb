package com.orientechnologies.orient.distributed.impl.structural.raft;

import com.orientechnologies.orient.distributed.OrientDBDistributed;
import com.orientechnologies.orient.distributed.impl.coordinator.OLogRequest;

public interface ORaftOperation extends OLogRequest {
  void apply(OrientDBDistributed context);
}
