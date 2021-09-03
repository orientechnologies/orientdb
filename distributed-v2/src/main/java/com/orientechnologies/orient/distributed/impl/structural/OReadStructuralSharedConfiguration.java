package com.orientechnologies.orient.distributed.impl.structural;

import com.orientechnologies.orient.core.db.config.ONodeIdentity;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collection;

public interface OReadStructuralSharedConfiguration {
  int getQuorum();

  boolean existsDatabase(String database);

  boolean existsNode(ONodeIdentity identity);

  boolean canAddNode(ONodeIdentity identity);

  OStructuralNodeConfiguration getNode(ONodeIdentity identity);

  Collection<OStructuralNodeConfiguration> listNodes();

  void networkDeserialize(DataInput input) throws IOException;

  void networkSerialize(DataOutput output) throws IOException;
}
