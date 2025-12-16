package com.orientechnologies.orient.distributed.context;

import com.orientechnologies.orient.core.transaction.ODatabaseId;
import com.orientechnologies.orient.core.transaction.ONodeId;
import com.orientechnologies.orient.core.tx.OTransactionSequenceStatus;
import com.orientechnologies.orient.distributed.context.coordination.message.ODatabaseMemberNetwork;
import com.orientechnologies.orient.distributed.context.coordination.message.ODatabaseStateNetwork;
import com.orientechnologies.orient.distributed.context.coordination.message.operation.OAddNodeInfo;
import com.orientechnologies.orient.distributed.context.coordination.result.OAcceptResult;
import com.orientechnologies.orient.distributed.context.coordination.result.OAlreadyPromised;
import com.orientechnologies.orient.distributed.context.coordination.result.OMissingNode;
import com.orientechnologies.orient.distributed.context.coordination.result.ONodeAlreadyPresent;
import com.orientechnologies.orient.distributed.context.coordination.result.OOutdatedVersion;
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
  private ODatabaseStateChangeListener stateListener;

  public ODatabaseTopologyState(
      ODatabaseId db,
      String name,
      Set<ONodeId> partecipants,
      int quorum,
      ODatabaseStateChangeListener stateListener) {
    this.id = db;
    this.name = name;
    for (ONodeId p : partecipants) {
      nodeStatus.put(p, new ONodeDatabaseState(p, ONodeRole.Main, ODatabaseState.Offline));
    }
    this.quorum = quorum;
    this.stateListener = stateListener;
  }

  public ODatabaseTopologyState(
      ODatabaseStateNetwork state, ODatabaseStateChangeListener stateListener) {
    this.id = state.getId();
    this.name = state.getName();
    this.stateListener = stateListener;
    this.receiveState(state);
  }

  public synchronized void defineNode(
      ONodeId node, ONodeRole role, ODatabaseState state, long version) {
    this.nodeStatus.computeIfAbsent(
        node,
        (n) -> {
          return new ONodeDatabaseState(n, role, state);
        });
    this.version = version;
    this.notifyChange(node, state);
  }

  public synchronized void setState(ONodeId node, ODatabaseState state, long version) {
    var no = this.nodeStatus.get(node);
    if (no != null) {
      no.setState(state);
    }
    this.version = version;
    this.promised = false;
    this.notifyChange(node, state);
  }

  public synchronized ODatabaseId getId() {
    return id;
  }

  public synchronized String getName() {
    return name;
  }

  public synchronized Optional<OAcceptResult> promiseState(
      ODatabaseState state, ONodeId nodeId, long version) {
    if (!this.nodeStatus.containsKey(nodeId)) {
      return Optional.of(new OMissingNode());
    }
    if (this.version + 1 == version) {
      if (promised) {
        return Optional.of(new OAlreadyPromised());
      } else {
        promised = true;
        return Optional.empty();
      }
    } else {
      return Optional.of(new OOutdatedVersion(this.version, version));
    }
  }

  public synchronized long getVersion() {
    return version;
  }

  public synchronized ODatabaseState getState(ONodeId nodeId) {
    var node = this.nodeStatus.get(nodeId);
    if (node != null) {
      return node.getState();
    } else {
      return ODatabaseState.NotAvailable;
    }
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
    return new ODatabaseStateNetwork(id, name, quorum, version, members);
  }

  public synchronized void receiveState(ODatabaseStateNetwork state) {
    // TODO: verify promised case ....
    if (this.version < state.getVersion()) {
      this.version = state.getVersion();
      this.quorum = state.getQuorum();
      for (ODatabaseMemberNetwork member : state.getMembers()) {
        ONodeDatabaseState status = this.nodeStatus.get(member.getNode());
        if (status != null) {
          if (status.getState() != member.getState()) {
            status.setState(member.getState());
            this.stateListener.onStateChange(id, member.getNode(), member.getState());
          }
          status.setRole(member.getRole());
        } else {
          var m = new ONodeDatabaseState(member.getNode(), member.getRole(), member.getState());
          this.nodeStatus.put(member.getNode(), m);
          this.stateListener.onStateChange(id, member.getNode(), member.getState());
        }
      }
    }
  }

  public synchronized void mergeState(ODatabaseStateNetwork state) {
    if (state.getQuorum() > this.quorum) {
      this.quorum = state.getQuorum();
    }
    for (ODatabaseMemberNetwork member : state.getMembers()) {
      ONodeDatabaseState status = this.nodeStatus.get(member.getNode());
      if (status != null) {
        if (status.getState() != member.getState()) {
          status.setState(member.getState());
          this.stateListener.onStateChange(id, member.getNode(), member.getState());
        }
        status.setRole(member.getRole());
      } else {
        var m = new ONodeDatabaseState(member.getNode(), member.getRole(), member.getState());
        this.nodeStatus.put(member.getNode(), m);
        this.stateListener.onStateChange(id, member.getNode(), member.getState());
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
      List<OAddNodeInfo> nodes, long version) {
    for (var node : nodes) {
      if (this.nodeStatus.containsKey(node.node())) {
        return Optional.of(new ONodeAlreadyPresent());
      }
    }
    if (this.version + 1 == version) {
      if (promised) {
        return Optional.of(new OAlreadyPromised());
      } else {
        promised = true;
        return Optional.empty();
      }
    } else {
      return Optional.of(new OOutdatedVersion(this.version, version));
    }
  }

  public synchronized void addMember(List<OAddNodeInfo> nodes, long version) {
    for (var node : nodes) {
      this.nodeStatus.put(
          node.node(), new ONodeDatabaseState(node.node(), node.role(), ODatabaseState.Offline));
      this.stateListener.onStateChange(id, node.node(), ODatabaseState.Offline);
    }
    this.promised = false;
  }

  public synchronized void cancelAddMemer(List<OAddNodeInfo> nodes) {
    this.promised = false;
  }

  public boolean shouldSink(ONodeId nodeID) {
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
}
