package com.orientechnologies.orient.distributed.context.coordination.dbs;

import com.orientechnologies.orient.core.transaction.ODatabaseId;
import com.orientechnologies.orient.core.transaction.ONodeId;
import com.orientechnologies.orient.core.transaction.OTransactionIdPromise;
import com.orientechnologies.orient.core.tx.OTransactionSequenceStatus;
import com.orientechnologies.orient.distributed.context.coordination.OVersion;
import com.orientechnologies.orient.distributed.context.coordination.OVersionPromise;
import com.orientechnologies.orient.distributed.context.coordination.message.operation.OAddNodeInfo;
import com.orientechnologies.orient.distributed.context.coordination.message.state.ODatabaseMemberNetwork;
import com.orientechnologies.orient.distributed.context.coordination.message.state.ODatabaseStateNetwork;
import com.orientechnologies.orient.distributed.context.coordination.result.OAcceptResult;
import com.orientechnologies.orient.distributed.context.coordination.result.OMissingNode;
import com.orientechnologies.orient.distributed.context.coordination.result.ONodeAlreadyPresent;
import com.orientechnologies.orient.distributed.context.coordination.sync.OSyncId;
import com.orientechnologies.orient.distributed.context.coordination.sync.OSyncInfo;
import com.orientechnologies.orient.distributed.context.coordination.sync.OSyncSession;
import com.orientechnologies.orient.distributed.context.coordination.sync.OSyncState;
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
  private final OVersionPromise versionPromise;
  private int quorum;
  private List<OActionNotification> notifications = new ArrayList<>();
  private Map<OSyncId, OSyncSession> syncSessions = new HashMap<>();
  private ODatabaseStateChangeListener stateListener;

  public ODatabaseTopologyState(
      ODatabaseId db,
      String name,
      Set<OAddNodeInfo> partecipants,
      int quorum,
      ODatabaseStateChangeListener stateListener,
      ONodeId current) {
    this.id = db;
    this.name = name;
    for (OAddNodeInfo p : partecipants) {
      nodeStatus.put(p.node(), new ONodeDatabaseState(p.node(), p.role(), ODatabaseState.Offline));
    }
    this.versionPromise = new OVersionPromise(new OVersion(0), current);
    this.quorum = quorum;
    this.stateListener = stateListener;
  }

  public ODatabaseTopologyState(
      ODatabaseStateNetwork state, ODatabaseStateChangeListener stateListener, ONodeId current) {
    this.id = state.id();
    this.name = state.name();
    this.stateListener = stateListener;
    this.versionPromise = new OVersionPromise(new OVersion(0), current);
    this.receiveState(state);
  }

  public ODatabaseTopologyState(
      ODatabaseStateChangeListener listener, ODatabaseTopologyStore store, ONodeId current) {
    this.stateListener = listener;
    this.id = store.getId();
    this.name = store.getName();
    this.quorum = store.getQuorum();
    this.versionPromise = new OVersionPromise(new OVersion(store.getVersion()), current);
    var nodes = store.getNodes().stream().map((x) -> new ONodeDatabaseState(x)).toList();
    for (var node : nodes) {
      this.nodeStatus.put(node.getId(), node);
    }
    this.stateListener = listener;
  }

  public synchronized void setState(
      ONodeId node, ODatabaseState state, long version, OTransactionIdPromise promise) {
    var no = this.nodeStatus.get(node);
    if (no != null) {
      no.setState(state);
    }
    this.versionPromise.accept(promise, new OVersion(version));
    this.notifyChange(node, state);
  }

  public synchronized ODatabaseId getId() {
    return id;
  }

  public synchronized String getName() {
    return name;
  }

  public synchronized Optional<OAcceptResult> promiseState(
      ODatabaseState state, ONodeId nodeId, long version, OTransactionIdPromise promise) {
    if (!this.nodeStatus.containsKey(nodeId)) {
      return Optional.of(new OMissingNode(nodeId));
    }
    return this.versionPromise.promise(promise, new OVersion(version));
  }

  public long getVersion() {
    return this.versionPromise.getVersion().getValue();
  }

  public synchronized ODatabaseState getState(ONodeId nodeId) {
    var node = this.nodeStatus.get(nodeId);
    if (node != null) {
      return node.getState();
    } else {
      return ODatabaseState.NotAvailable;
    }
  }

  public synchronized void cancelSetState(
      ONodeId nodeId, long version, OTransactionIdPromise promise) {
    this.versionPromise.cancel(promise);
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
     * Return false to wait true to execute
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
      long start = currentTime();
      long till = start + timeOut;
      while (!cond.match() && timeOut > 0) {
        this.wait(timeOut);
        long current = currentTime();
        timeOut = till - current;
      }
      return timeOut > 0;
    } else {
      while (!cond.match()) {
        this.wait();
      }
      return true;
    }
  }

  private long currentTime() {
    return System.nanoTime() / 1000;
  }

  private void executeOn(WaitCond cond, OStateAction execute) {
    this.notifications.add(new OActionNotification(cond, execute));
  }

  private void notifyChange(ONodeId node, ODatabaseState state) {
    Iterator<OActionNotification> iter = this.notifications.iterator();
    while (iter.hasNext()) {
      OActionNotification act = iter.next();
      if (act.cond.match()) {
        act.execute.execute();
        iter.remove();
      }
    }
    this.stateListener.onStateChange(id, node, state);
    this.notifyAll();
  }

  public synchronized Set<ONodeId> getOnlineNodes() {
    return nodeStatus.values().stream()
        .filter((x) -> x.isOnline())
        .map((x) -> x.getId())
        .collect(Collectors.toSet());
  }

  public synchronized Optional<OSyncInfo> newSync() {
    if (!syncSessions.isEmpty()) {
      return Optional.empty();
    }
    Set<ONodeId> onlineNodes = getOnlineNodes();
    OSyncSession session = new OSyncSession(getId(), onlineNodes);
    this.syncSessions.put(session.getSyncId(), session);
    return Optional.of(new OSyncInfo(session.getSyncId(), onlineNodes));
  }

  public synchronized Optional<OSyncState> canSync(
      ONodeId sender,
      ONodeId receiver,
      OSyncId syncId,
      boolean canSync,
      OSyncMode mode,
      Optional<OTransactionSequenceStatus> sequenceStatus) {
    return this.syncSessions
        .get(syncId)
        .canSync(sender, receiver, syncId, canSync, mode, sequenceStatus);
  }

  public synchronized OSyncState startSend(
      ONodeId from,
      ONodeId to,
      OSyncId syncId,
      OSyncMode mode,
      Optional<OTransactionSequenceStatus> sequenceStatus) {
    OSyncSession session = new OSyncSession(getId(), syncId, from, to, mode, sequenceStatus);
    this.syncSessions.put(syncId, session);
    return session.getState();
  }

  public synchronized boolean acceptSync(ONodeId sender, ONodeId receiver, OSyncId syncId) {
    for (OSyncSession session : this.syncSessions.values()) {
      if (session.isTransferingData()) {
        return false;
      }
    }
    return this.nodeStatus.get(sender).isOnline();
  }

  public boolean isMain(ONodeId nodeId) {
    ONodeDatabaseState stat = this.nodeStatus.get(nodeId);
    if (stat != null) {
      return stat.isMain();
    }
    return false;
  }

  public synchronized ODatabaseStateNetwork getNetworkState() {
    List<ODatabaseMemberNetwork> members = new ArrayList<>();
    for (ONodeDatabaseState state : this.nodeStatus.values()) {
      members.add(state.getNetworkState());
    }
    return new ODatabaseStateNetwork(id, name, quorum, getVersion(), members);
  }

  public synchronized void receiveState(ODatabaseStateNetwork state) {
    // TODO: verify promised case ....
    if (this.getVersion() < state.version()) {
      this.versionPromise.loadVersion(new OVersion(state.version()));
      this.quorum = state.quorum();
      for (ODatabaseMemberNetwork member : state.members()) {
        ONodeDatabaseState status = this.nodeStatus.get(member.node());
        if (status != null) {
          if (status.getState() != member.state()) {
            status.setState(member.state());
            this.stateListener.onStateChange(id, member.node(), member.state());
          }
          status.setRole(member.role());
        } else {
          var m = new ONodeDatabaseState(member.node(), member.role(), member.state());
          this.nodeStatus.put(member.node(), m);
          this.stateListener.onStateChange(id, member.node(), member.state());
        }
      }
    }
  }

  public synchronized void mergeState(ODatabaseStateNetwork state) {
    if (state.quorum() > this.quorum) {
      this.quorum = state.quorum();
    }
    for (ODatabaseMemberNetwork member : state.members()) {
      ONodeDatabaseState status = this.nodeStatus.get(member.node());
      if (status != null) {
        if (status.getState() != member.state()) {
          status.setState(member.state());
          this.stateListener.onStateChange(id, member.node(), member.state());
        }
        status.setRole(member.role());
      } else {
        var m = new ONodeDatabaseState(member.node(), member.role(), member.state());
        this.nodeStatus.put(member.node(), m);
        this.stateListener.onStateChange(id, member.node(), member.state());
      }
    }
  }

  public synchronized ONodeRole getRole(ONodeId nodeId) {
    ONodeDatabaseState stat = this.nodeStatus.get(nodeId);
    if (stat != null) {
      return stat.getRole();
    }
    return null;
  }

  public synchronized Optional<OAcceptResult> promiseMember(
      List<OAddNodeInfo> nodes, long version, OTransactionIdPromise promise) {
    for (var node : nodes) {
      if (this.nodeStatus.containsKey(node.node())) {
        return Optional.of(new ONodeAlreadyPresent(this.id, node.node()));
      }
    }
    return this.versionPromise.promise(promise, new OVersion(version));
  }

  public synchronized void addMember(
      List<OAddNodeInfo> nodes, long version, OTransactionIdPromise promise) {
    for (var node : nodes) {
      this.nodeStatus.put(
          node.node(), new ONodeDatabaseState(node.node(), node.role(), ODatabaseState.Offline));
      this.stateListener.onStateChange(id, node.node(), ODatabaseState.Offline);
    }
    this.versionPromise.accept(promise, new OVersion(version));
  }

  public synchronized void cancelAddMemer(List<OAddNodeInfo> nodes, OTransactionIdPromise promise) {
    this.versionPromise.cancel(promise);
  }

  public synchronized boolean shouldSink(ONodeId nodeID) {
    if (ODatabaseState.Offline.equals(getState(nodeID))) {
      for (var state : this.nodeStatus.values()) {
        if (ODatabaseState.Online.equals(state.getState())) {
          return true;
        }
      }
      return false;
    } else {
      return false;
    }
  }

  public synchronized void completeSync(OSyncId syncId) {
    syncSessions.remove(syncId);
  }

  public ODatabaseTopologyStore getStore() {
    var nodes = this.nodeStatus.values().stream().map((x) -> x.toStore()).toList();
    return new ODatabaseTopologyStore(nodes, this.id, this.name, this.getVersion(), this.quorum);
  }
}
