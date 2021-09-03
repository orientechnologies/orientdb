package com.orientechnologies.orient.distributed.impl.structural.raft;

import com.orientechnologies.orient.distributed.OrientDBDistributed;
import com.orientechnologies.orient.distributed.impl.coordinator.transaction.OSessionOperationId;
import com.orientechnologies.orient.distributed.impl.log.OLogRequest;
import java.util.Optional;

public interface ORaftOperation extends OLogRequest {
  void apply(OrientDBDistributed context);

  Optional<OSessionOperationId> getRequesterSequential();
}
