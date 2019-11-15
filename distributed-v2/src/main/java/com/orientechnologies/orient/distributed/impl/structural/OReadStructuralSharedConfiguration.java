package com.orientechnologies.orient.distributed.impl.structural;

import com.orientechnologies.common.util.OSizeable;
import com.orientechnologies.orient.core.db.config.ONodeIdentity;

import java.util.Collection;

public interface OReadStructuralSharedConfiguration {
  int getQuorum();

  boolean existsDatabase(String database);

  boolean existsNode(ONodeIdentity identity);

  boolean canAddNode(ONodeIdentity identity);

  OStructuralNodeConfiguration getNode(ONodeIdentity identity);

  Collection<OStructuralNodeConfiguration> listNodes();
}
