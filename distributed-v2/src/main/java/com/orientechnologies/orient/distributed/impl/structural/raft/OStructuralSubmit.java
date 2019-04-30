package com.orientechnologies.orient.distributed.impl.structural.raft;

import com.orientechnologies.orient.distributed.impl.coordinator.transaction.OSessionOperationId;

public interface OStructuralSubmit {
  void begin(OSessionOperationId id, OMasterContext context);
}
