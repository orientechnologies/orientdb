package com.orientechnologies.orient.distributed.context;

import com.orientechnologies.orient.core.transaction.ODatabaseId;
import com.orientechnologies.orient.core.transaction.ONodeId;
import com.orientechnologies.orient.distributed.db.OSyncMode;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

public class OSyncSession {
  private final UUID syncId;
  private final ODatabaseId dbId;
  private Set<ONodeId> nodes;
  private OSyncState state;

  public OSyncSession(ODatabaseId dbId, Set<ONodeId> nodes) {
    this.dbId = dbId;
    this.syncId = UUID.randomUUID();
    this.nodes = nodes;
  }

  public OSyncSession(ODatabaseId dbId, UUID syncId, ONodeId from, ONodeId to, OSyncMode mode) {
    this.syncId = syncId;
    this.dbId = dbId;
    this.state = new OSyncState(dbId, syncId, from, to, mode);
  }

  public UUID getSyncId() {
    return syncId;
  }

  public Optional<OSyncState> canSync(
      ONodeId from, ONodeId to, UUID syncId, boolean canSync, OSyncMode mode) {
    assert this.syncId.equals(syncId);
    if (canSync) {
      this.state = new OSyncState(dbId, syncId, from, to, mode);
      return Optional.of(this.state);
    } else {
      nodes.remove(from);
      // TODO: if reach 0 terminate
      return Optional.empty();
    }
  }

  public OSyncState getState() {
    return state;
  }
}
