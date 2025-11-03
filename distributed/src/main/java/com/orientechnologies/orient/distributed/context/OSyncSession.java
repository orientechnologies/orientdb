package com.orientechnologies.orient.distributed.context;

import com.orientechnologies.orient.core.transaction.ODatabaseId;
import com.orientechnologies.orient.core.transaction.ONodeId;
import com.orientechnologies.orient.distributed.db.OSyncMode;
import java.util.Optional;
import java.util.Set;

public class OSyncSession {
  private final OSyncId syncId;
  private final ODatabaseId dbId;
  private Set<ONodeId> nodes;
  private OSyncState state;

  public OSyncSession(ODatabaseId dbId, Set<ONodeId> nodes) {
    this.dbId = dbId;
    this.syncId = new OSyncId();
    this.nodes = nodes;
  }

  public OSyncSession(ODatabaseId dbId, OSyncId syncId, ONodeId from, ONodeId to, OSyncMode mode) {
    this.syncId = syncId;
    this.dbId = dbId;
    this.state = new OSyncState(dbId, syncId, from, to, mode);
  }

  public OSyncId getSyncId() {
    return syncId;
  }

  public Optional<OSyncState> canSync(
      ONodeId sender, ONodeId receiver, OSyncId syncId, boolean canSync, OSyncMode mode) {
    assert this.syncId.equals(syncId);
    if (canSync) {
      this.state = new OSyncState(dbId, syncId, sender, receiver, mode);
      return Optional.of(this.state);
    } else {
      nodes.remove(sender);
      // TODO: if reach 0 terminate
      return Optional.empty();
    }
  }

  public boolean isTransferingData() {
    return this.state != null;
  }

  public OSyncState getState() {
    return state;
  }
}
