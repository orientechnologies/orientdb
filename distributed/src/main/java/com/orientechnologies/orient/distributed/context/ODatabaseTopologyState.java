package com.orientechnologies.orient.distributed.context;

import com.orientechnologies.orient.core.transaction.ODatabaseId;
import com.orientechnologies.orient.core.transaction.ONodeId;
import com.orientechnologies.orient.distributed.context.coordination.result.OAcceptResult;
import com.orientechnologies.orient.distributed.context.coordination.result.OAlreadyPromised;
import com.orientechnologies.orient.distributed.context.coordination.result.OInvalidSequential;
import com.orientechnologies.orient.distributed.db.OSyncMode;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public class ODatabaseTopologyState {
  private final ODatabaseId id;
  private final String name;
  private final Map<ONodeId, ONodeDatabaseState> nodeStatus = new HashMap<>();
  private long version = 0;
  private boolean promised = false;
  private int quorum;
  private List<OActionNotification> notifications = new ArrayList<>();
  private Map<OSyncId, OSyncSession> syncSessions = new HashMap<>();

  public ODatabaseTopologyState(
      ODatabaseId db, String name, Set<ONodeId> partecipants, int quorum) {
    this.id = db;
    this.name = name;
    for (ONodeId p : partecipants) {
      nodeStatus.put(p, new ONodeDatabaseState(p, ONodeRole.Main, ODatabaseState.Offline));
    }
    this.quorum = quorum;
  }

  public synchronized void defineNode(
      ONodeId node, ONodeRole role, ODatabaseState state, long version) {
    this.nodeStatus.computeIfAbsent(
        node,
        (n) -> {
          return new ONodeDatabaseState(n, role, state);
        });
    this.version = version;
    this.notifyChange();
  }

  public synchronized void setState(ONodeId node, ODatabaseState state, long version) {
    var no = this.nodeStatus.get(node);
    if (no != null) {
      no.setState(state);
    }
    this.version = version;
    this.promised = false;
    this.notifyChange();
  }

  public synchronized ODatabaseId getId() {
    return id;
  }

  public synchronized String getName() {
    return name;
  }

  public synchronized Optional<OAcceptResult> promiseState(
      ODatabaseState state, ONodeId nodeId, long version) {
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

  public synchronized long getVersion() {
    return version;
  }

  public synchronized ODatabaseState getState(ONodeId nodeId) {
    return this.nodeStatus.get(nodeId).getState();
  }

  public synchronized void cancelSetState(ONodeId nodeId, long version) {
    if (this.version + 1 == version && promised) {
      this.promised = false;
    }
  }

  public synchronized void executeOnOneOnline() throws InterruptedException {
    executeOn(this::isOneOnline, () -> {});
  }

  public synchronized boolean waitOnlineQuorum(Optional<Long> timeout) throws InterruptedException {
    return waitFor(timeout, this::isQuorumOnline);
  }

  private boolean isOneOnline() {
    long online = this.nodeStatus.values().stream().filter((x) -> x.isOnline()).count();
    return online > 0;
  }

  private boolean isQuorumOnline() {
    long online = this.nodeStatus.values().stream().filter((x) -> x.isOnline()).count();
    return online >= quorum;
  }

  public boolean waitOnlineOne() {
    return false;
  }

  private interface WaitCond {
    /*
     *  Return false to wait true to execute
     */
    boolean match();
  }

  public synchronized void executeOnOneOnline(OStateAction execute) {
    executeOn(this::isOneOnline, execute);
  }

  private record OActionNotification(WaitCond cond, OStateAction execute) {}

  private boolean waitFor(Optional<Long> timeout, WaitCond cond) throws InterruptedException {
    if (timeout.isPresent()) {
      var timeOut = timeout.get();
      long till = System.currentTimeMillis() + timeOut;
      boolean stillTime = true;
      while (!cond.match() && stillTime) {
        this.wait(timeout.get());
        stillTime = till > System.currentTimeMillis();
      }
      return stillTime;
    } else {
      while (!cond.match()) {
        this.wait();
      }
      return true;
    }
  }

  private void executeOn(WaitCond cond, OStateAction execute) {
    this.notifications.add(new OActionNotification(cond, execute));
  }

  private void notifyChange() {
    Iterator<OActionNotification> iter = this.notifications.iterator();
    while (iter.hasNext()) {
      OActionNotification act = iter.next();
      if (act.cond.match()) {
        act.execute.execute();
        iter.remove();
      }
    }
    this.notifyAll();
  }

  public synchronized Set<ONodeId> getOnlineNodes() {
    return nodeStatus.values().stream()
        .filter((x) -> x.isOnline())
        .map((x) -> x.getId())
        .collect(Collectors.toSet());
  }

  public synchronized OSyncInfo newSync() {
    Set<ONodeId> onlineNodes = getOnlineNodes();
    OSyncSession session = new OSyncSession(getId(), onlineNodes);
    this.syncSessions.put(session.getSyncId(), session);
    return new OSyncInfo(session.getSyncId(), onlineNodes);
  }

  public synchronized Optional<OSyncState> canSync(
      ONodeId from, ONodeId to, OSyncId syncId, boolean canSync, OSyncMode mode) {
    return this.syncSessions.get(syncId).canSync(from, to, syncId, canSync, mode);
  }

  public OSyncState startSend(ONodeId from, ONodeId to, OSyncId syncId, OSyncMode mode) {
    OSyncSession session = new OSyncSession(getId(), syncId, from, to, mode);
    this.syncSessions.put(syncId, session);
    return session.getState();
  }
}
