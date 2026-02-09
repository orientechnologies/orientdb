package com.orientechnologies.orient.distributed.context.coordination.sync;

import com.orientechnologies.orient.core.transaction.ODatabaseId;
import com.orientechnologies.orient.core.transaction.ONodeId;
import com.orientechnologies.orient.core.tx.OTransactionSequenceStatus;
import com.orientechnologies.orient.distributed.db.OSyncMode;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

public class OSyncSession {
  private final OSyncId syncId;
  private final ODatabaseId dbId;
  private Set<ONodeId> nodes;
  private OSyncState state;
  private CompletableFuture<Boolean> finished = new CompletableFuture<Boolean>();

  public OSyncSession(ODatabaseId dbId, Set<ONodeId> nodes) {
    this.dbId = dbId;
    this.syncId = new OSyncId();
    this.nodes = nodes;
  }

  public OSyncSession(
      ODatabaseId dbId,
      OSyncId syncId,
      ONodeId from,
      ONodeId to,
      OSyncMode mode,
      Optional<OTransactionSequenceStatus> sequenceStatus) {
    this.syncId = syncId;
    this.dbId = dbId;
    this.state = new OSyncState(dbId, syncId, from, to, mode, sequenceStatus, finished);
  }

  public OSyncId getSyncId() {
    return syncId;
  }

  public Optional<OSyncState> canSync(
      ONodeId sender,
      ONodeId receiver,
      OSyncId syncId,
      boolean canSync,
      OSyncMode mode,
      Optional<OTransactionSequenceStatus> sequenceStatus) {
    assert this.syncId.equals(syncId);
    if (isTransferingData()) {
      return Optional.empty();
    }
    if (canSync) {
      this.state = new OSyncState(dbId, syncId, sender, receiver, mode, sequenceStatus, finished);
      return Optional.of(this.state);
    } else {
      nodes.remove(sender);
      // TODO: if reach remove also from the sync session map
      if (nodes.isEmpty()) {
        this.finished.complete(false);
      }
      return Optional.empty();
    }
  }

  public boolean isTransferingData() {
    if (this.state != null) {
      return !this.state.isClose();
    } else {
      return false;
    }
  }

  public OSyncState getState() {
    return state;
  }

  public Future<Boolean> getFinished() {
    return finished;
  }
}
