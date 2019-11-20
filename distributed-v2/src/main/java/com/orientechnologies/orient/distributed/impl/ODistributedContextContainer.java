package com.orientechnologies.orient.distributed.impl;

import com.orientechnologies.orient.core.db.config.ONodeIdentity;
import com.orientechnologies.orient.distributed.impl.coordinator.OLogId;
import com.orientechnologies.orient.distributed.impl.metadata.ODistributedContext;
import com.orientechnologies.orient.distributed.impl.structural.OStructuralDistributedContext;

public interface ODistributedContextContainer {
  void checkDatabaseReady(String database);

  ODistributedContext getDistributedContext(String database);

  OStructuralDistributedContext getStructuralDistributedContext();

  void nodeDisconnected(ONodeIdentity identity);

  void nodeConnected(ONodeIdentity identity);

  void setLeader(ONodeIdentity leader, OLogId lastValid);

}
