package com.orientechnologies.orient.distributed.context;

import com.orientechnologies.orient.core.transaction.ONodeId;
import java.util.HashMap;
import java.util.Map;

public class ODatabaseTopologyState {
  private final ODatabaseId id;
  private final String name;
  private final Map<ONodeId, ONodeDatabaseState> nodeStatus = new HashMap<>();
  private int version = 0;

  public ODatabaseTopologyState(ODatabaseId db, String name) {
    this.id = db;
    this.name = name;
  }

  public void defineNode(ONodeId node, ONodeRole role, ODatabaseState state, int version) {
    this.nodeStatus.computeIfAbsent(
        node,
        (n) -> {
          return new ONodeDatabaseState(n, role, state);
        });
    this.version = version;
  }

  public void setState(ONodeId node, ODatabaseState state, int version) {
    var no = this.nodeStatus.get(node);
    if (no != null) {
      no.setState(state);
    }
    this.version = version;
  }

  public ODatabaseId getId() {
    return id;
  }

  public String getName() {
    return name;
  }
}
