package com.orientechnologies.orient.distributed.context;

import com.orientechnologies.orient.core.transaction.ODatabaseId;
import com.orientechnologies.orient.core.transaction.ONodeId;
import com.orientechnologies.orient.distributed.context.coordination.result.OAcceptResult;
import com.orientechnologies.orient.distributed.context.coordination.result.OInvalidSequential;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class ODatabaseTopologyState {
  private final ODatabaseId id;
  private final String name;
  private final Map<ONodeId, ONodeDatabaseState> nodeStatus = new HashMap<>();
  private long version = 0;

  public ODatabaseTopologyState(ODatabaseId db, String name) {
    this.id = db;
    this.name = name;
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
  }

  public ODatabaseId getId() {
    return id;
  }

  public String getName() {
    return name;
  }

  public Optional<OAcceptResult> promiseState(ODatabaseState state, ONodeId nodeId, long version) {
    if (this.version + 1 == version) {
      return Optional.empty();
    } else {
      return Optional.of(new OInvalidSequential());
    }
  }

  public long getVersion() {
    return version;
  }
}
