package com.orientechnologies.orient.distributed.impl.structural.raft;

import com.orientechnologies.orient.core.db.config.ONodeIdentity;
import com.orientechnologies.orient.distributed.impl.coordinator.transaction.OSessionOperationId;
import java.util.Optional;

public interface OStructuralSubmit {
  void begin(Optional<ONodeIdentity> requester, OSessionOperationId id, OLeaderContext context);
}
