package com.orientechnologies.orient.distributed.context.coordination.sync;

import com.orientechnologies.orient.core.transaction.ODatabaseId;
import com.orientechnologies.orient.core.transaction.ONodeId;
import com.orientechnologies.orient.core.tx.OTransactionSequenceStatus;
import com.orientechnologies.orient.distributed.context.coordination.sync.OSyncState.SyncComplete;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

public class OSyncSession implements SyncComplete {
  private final OSyncId syncId;
  private final ODatabaseId dbId;
  private Set<ONodeId> nodes;
  private OSyncState state;
  private CompletableFuture<Boolean> finished = new CompletableFuture<Boolean>();

  public OSyncSession(ODatabaseId dbId, ONodeId current, Set<ONodeId> nodes) {
    this.dbId = dbId;
    this.syncId = new OSyncId(dbId, current);
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
    this.state = new OSyncState(dbId, syncId, from, to, mode, sequenceStatus, this);
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
    if (canSync && this.state == null) {
      this.state = new OSyncState(dbId, syncId, sender, receiver, mode, sequenceStatus, this);
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

  public boolean isFinished() {
    if (this.state != null) {
      return this.state.isClose();
    } else {
      return nodes.isEmpty();
    }
  }

  public OSyncState getState() {
    return state;
  }

  public Future<Boolean> getFinished() {
    return finished;
  }

  public boolean nodeDisconnected(ONodeId node) {
    if (state != null) {
      if (node.equals(state.getSender()) || node.equals(state.getReceiver())) {
        state.close();
        return true;
      } else {
        return false;
      }
    } else {
      nodes.remove(node);
      if (nodes.isEmpty()) {
        this.complete(false);
        return true;
      } else {
        return false;
      }
    }
  }

  public void complete(boolean success) {
    this.finished.complete(success);
  }
}
