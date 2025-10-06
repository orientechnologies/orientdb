package com.orientechnologies.orient.distributed.context;

import com.orientechnologies.orient.core.transaction.ODatabaseId;
import com.orientechnologies.orient.core.transaction.ONodeId;
import com.orientechnologies.orient.distributed.context.coordination.result.OAcceptResult;
import com.orientechnologies.orient.distributed.context.coordination.result.OAlreadyPromised;
import com.orientechnologies.orient.distributed.context.coordination.result.OInvalidSequential;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public class ODatabaseTopologyState {
  private final ODatabaseId id;
  private final String name;
  private final Map<ONodeId, ONodeDatabaseState> nodeStatus = new HashMap<>();
  private long version = 0;
  private boolean promised = false;

  public ODatabaseTopologyState(ODatabaseId db, String name, Set<ONodeId> partecipants) {
    this.id = db;
    this.name = name;
    for (ONodeId p : partecipants) {
      nodeStatus.put(p, new ONodeDatabaseState(p, ONodeRole.Main, ODatabaseState.Offline));
    }
  }

  public void defineNode(ONodeId node, ONodeRole role, ODatabaseState state, long version) {
    this.nodeStatus.computeIfAbsent(
        node,
        (n) -> {
          return new ONodeDatabaseState(n, role, state);
        });
    this.version = version;
  }

  public void setState(ONodeId node, ODatabaseState state, long version) {
    var no = this.nodeStatus.get(node);
    if (no != null) {
      no.setState(state);
    }
    this.version = version;
    this.promised = false;
  }

  public ODatabaseId getId() {
    return id;
  }

  public String getName() {
    return name;
  }

  public Optional<OAcceptResult> promiseState(ODatabaseState state, ONodeId nodeId, long version) {
    if (this.version + 1 == version) {
      if (promised) {
        return Optional.of(new OAlreadyPromised());
      } else {
        promised = true;
        return Optional.empty();
      }
    } else {
      return Optional.of(new OInvalidSequential());
    }
  }

  public long getVersion() {
    return version;
  }

  public ODatabaseState getState(ONodeId nodeId) {
    return this.nodeStatus.get(nodeId).getState();
  }

  public void cancelSetState(ONodeId nodeId, long version) {
    if (this.version + 1 == version && promised) {
      this.promised = false;
    }
  }
}
