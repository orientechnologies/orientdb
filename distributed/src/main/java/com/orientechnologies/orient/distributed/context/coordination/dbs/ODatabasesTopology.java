package com.orientechnologies.orient.distributed.context.coordination.dbs;

import com.orientechnologies.orient.core.transaction.ODatabaseId;
import com.orientechnologies.orient.core.transaction.ONodeId;
import com.orientechnologies.orient.distributed.context.coordination.OVersion;
import com.orientechnologies.orient.distributed.context.coordination.sync.OSyncState;
import java.util.Collection;
import java.util.Optional;
import java.util.Set;

public interface ODatabasesTopology {

  Collection<ODatabaseId> getDatabases();

  ODatabaseState getState(ODatabaseId dbId, ONodeId node);

  boolean isOnline(ODatabaseId dbId, ONodeId nodeID);

  boolean isSynching(ODatabaseId dbId);

  boolean shouldSink(ODatabaseId dbId, ONodeId nodeID);

  OVersion getDatabaseVersion(ODatabaseId dbId);

  String getDatabaseName(ODatabaseId dbId);

  ONodeRole getRole(ODatabaseId dbId, ONodeId node);

  Optional<ODatabaseId> getDatabaseId(String dbName);

  Set<ONodeId> getOnlineNodes(ODatabaseId dbId);

  boolean isMain(ODatabaseId dbId, ONodeId node);

  public Set<OSyncState> getActiveSyncs(ODatabaseId dbId);

  Set<ONodeId> getMembers(ODatabaseId databaseId);

  int getQuorum(ODatabaseId databaseId);

  boolean isQuorumOnline(ODatabaseId dbId);

  boolean isAllOnline(ODatabaseId dbId);
}
