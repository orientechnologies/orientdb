package com.orientechnologies.orient.distributed.context.coordination.sync;

import com.orientechnologies.orient.core.transaction.ODatabaseId;
import com.orientechnologies.orient.core.transaction.ONodeId;
import com.orientechnologies.orient.distributed.context.coordination.dbs.OCanSyncResult;
import com.orientechnologies.orient.distributed.context.coordination.message.OCanSyncAccept;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

public class OSyncSession {
  private final OSyncId syncId;
  private final ODatabaseId dbId;
  private Set<ONodeId> nodes;
  private Optional<OSyncState> state = Optional.empty();
  private CompletableFuture<Boolean> finished = new CompletableFuture<Boolean>();
  private long start = System.nanoTime();

  public OSyncSession(ODatabaseId dbId, ONodeId current, Set<ONodeId> nodes) {
    this.dbId = dbId;
    this.syncId = new OSyncId(dbId, current);
    this.nodes = nodes;
  }

  public OSyncSession(ODatabaseId dbId, OSyncId syncId) {
    this.syncId = syncId;
    this.dbId = dbId;
  }

  public OSyncId getSyncId() {
    return syncId;
  }

  public Optional<OSyncState> startSync(
      ONodeId sender, ONodeId receiver, OSyncId syncId, OCanSyncAccept canSync) {
    assert this.syncId.equals(syncId);
    if (canSync.isSync() && this.state.isEmpty()) {
      this.state = Optional.of(new OSyncState(dbId, syncId, sender, receiver, canSync));
      return this.state;
    } else {
      nodes.remove(sender);
      if (nodes.isEmpty()) {
        this.finished.complete(false);
      }
      return Optional.empty();
    }
  }

  public Optional<OCanSyncResult> canSync(
      ONodeId sender, ONodeId receiver, OSyncId syncId, OCanSyncAccept canSync) {
    assert this.syncId.equals(syncId);
    if (canSync.isSync()) {
      if (this.state.isEmpty()) {
        this.state = Optional.of(new OSyncState(dbId, syncId, sender, receiver, canSync));
        return Optional.of(new OCanSyncResult(this.state.get(), new HashSet<>(this.nodes)));
      } else {
        return Optional.empty();
      }
    } else {
      nodes.remove(sender);
      if (nodes.isEmpty()) {
        this.finished.complete(false);
      }
      return Optional.empty();
    }
  }

  public boolean isTransferingData() {
    return this.state.map(OSyncState::isClose).map(v -> !v).orElse(false);
  }

  public boolean isFinished() {
    return this.state.map(OSyncState::isClose).orElse(nodes.isEmpty());
  }

  public Optional<OSyncState> getState() {
    return state;
  }

  public Future<Boolean> getFinished() {
    return finished;
  }

  public boolean nodeDisconnected(ONodeId node) {
    if (state.isPresent()) {
      var st = state.get();
      if (node.equals(st.getSender()) || node.equals(st.getReceiver())) {
        st.close();
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

  public boolean checkTimeout(long timeOutSync) {
    long lastOp = state.map(OSyncState::getLastTimeMessageReceived).orElse(start);
    var now = System.nanoTime();
    if ((lastOp - now) / 1000 > timeOutSync) {
      state.ifPresent(OSyncState::close);
      complete(false);
      return true;
    }
    return false;
  }

  @Override
  public String toString() {
    return "OSyncSession [syncId="
        + syncId
        + ", start="
        + start
        + ", transfering="
        + isTransferingData()
        + ", terminated="
        + isFinished()
        + " ]";
  }
}
