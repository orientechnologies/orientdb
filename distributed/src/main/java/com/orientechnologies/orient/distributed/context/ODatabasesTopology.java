package com.orientechnologies.orient.distributed.context;

import com.orientechnologies.orient.core.transaction.ODatabaseId;
import com.orientechnologies.orient.core.transaction.ONodeId;
import java.util.Collection;
import java.util.Optional;
import java.util.Set;

public interface ODatabasesTopology {

  Collection<ODatabaseId> getDatabases();

  ODatabaseState getState(ODatabaseId dbId, ONodeId node);

  long getDatabaseVersion(ODatabaseId dbId);

  String getDatabaseName(ODatabaseId dbId);

  ONodeRole getRole(ODatabaseId dbId, ONodeId node);

  Optional<ODatabaseId> getDatabaseId(String dbName);

  Set<ONodeId> getOnlineNodes(ODatabaseId oDatabaseId);

  boolean isMain(ODatabaseId dbId, ONodeId node);
}
