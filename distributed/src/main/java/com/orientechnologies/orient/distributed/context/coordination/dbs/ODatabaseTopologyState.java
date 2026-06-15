package com.orientechnologies.orient.distributed.context.coordination.dbs;

import com.orientechnologies.orient.core.transaction.ODatabaseId;
import com.orientechnologies.orient.core.transaction.ONodeId;
import com.orientechnologies.orient.core.transaction.OTransactionIdPromise;
import com.orientechnologies.orient.distributed.context.coordination.OVersion;
import com.orientechnologies.orient.distributed.context.coordination.OVersionPromise;
import com.orientechnologies.orient.distributed.context.coordination.message.OCanSyncAccept;
import com.orientechnologies.orient.distributed.context.coordination.message.operation.OAddNodeInfo;
import com.orientechnologies.orient.distributed.context.coordination.message.state.ODatabaseMemberNetwork;
import com.orientechnologies.orient.distributed.context.coordination.message.state.ODatabaseStateNetwork;
import com.orientechnologies.orient.distributed.context.coordination.result.OAcceptResult;
import com.orientechnologies.orient.distributed.context.coordination.result.ODatabaseSynching;
import com.orientechnologies.orient.distributed.context.coordination.result.OMissingNode;
import com.orientechnologies.orient.distributed.context.coordination.result.ONodeAlreadyPresent;
import com.orientechnologies.orient.distributed.context.coordination.result.OQuormuTooBig;
import com.orientechnologies.orient.distributed.context.coordination.result.OQuormuTooSmall;
import com.orientechnologies.orient.distributed.context.coordination.sync.OSyncId;
import com.orientechnologies.orient.distributed.context.coordination.sync.OSyncInfo;
import com.orientechnologies.orient.distributed.context.coordination.sync.OSyncSession;
import com.orientechnologies.orient.distributed.context.coordination.sync.OSyncState;
import com.orientechnologies.orient.server.distributed.OLoggerDistributed;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public class ODatabaseTopologyState extends OWatcher {
  private static final OLoggerDistributed logger =
      OLoggerDistributed.logger(ODatabaseTopologyState.class);
  private final ODatabaseId id;
  private final ONodeId current;
  private final String name;
  private final Map<ONodeId, ONodeDatabaseState> nodeStatus = new HashMap<>();
  private final OVersionPromise versionPromise;
  private int quorum;
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
    this.versionPromise = new OVersionPromise(new OVersion(1), current);
    this.quorum = quorum;
    this.stateListener = stateListener;
    this.current = current;
  }

  public ODatabaseTopologyState(
      ODatabaseStateNetwork state, ODatabaseStateChangeListener stateListener, ONodeId current) {
    this.id = state.id();
    this.name = state.name();
    this.stateListener = stateListener;
    this.versionPromise = new OVersionPromise(new OVersion(0), current);
    this.current = current;
    this.receiveState(state, false);
  }

  public ODatabaseTopologyState(
      ODatabaseStateChangeListener listener, ODatabaseTopologyStore store, ONodeId current) {
    this.stateListener = listener;
    this.id = store.id();
    this.name = store.name();
    this.quorum = store.quorum();
    this.current = current;
    this.versionPromise = new OVersionPromise(store.version(), current);
    var nodes = store.nodes().stream().map((x) -> new ONodeDatabaseState(x)).toList();
    for (var node : nodes) {
      this.nodeStatus.put(node.getId(), node);
    }
    this.stateListener = listener;
  }

  public synchronized void setState(
      ONodeId node, ODatabaseState state, OVersion version, OTransactionIdPromise promise) {
    var no = this.nodeStatus.get(node);
    if (no != null) {
      no.setState(state);
    }
    this.versionPromise.accept(promise, version);
    this.notifyChange(node, state);
  }

  public synchronized ODatabaseId getId() {
    return id;
  }

  public synchronized String getName() {
    return name;
  }

  public synchronized Optional<OAcceptResult> promiseState(
      ODatabaseState state, ONodeId nodeId, OVersion version, OTransactionIdPromise promise) {
    if (!this.nodeStatus.containsKey(nodeId)) {
      return Optional.of(new OMissingNode(nodeId));
    }
    return this.versionPromise.promise(promise, version);
  }

  public OVersion getVersion() {
    return this.versionPromise.getVersion();
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
      ONodeId nodeId, OVersion version, OTransactionIdPromise promise) {
    this.versionPromise.cancel(promise);
  }

  public synchronized boolean waitOnlineQuorum(Optional<Long> timeout) throws InterruptedException {
    return waitFor(timeout, this::isQuorumOnline);
  }

  public synchronized boolean waitOnlineAll(Optional<Long> timeout) throws InterruptedException {
    return waitFor(timeout, this::isAllOnline);
  }

  private boolean isOneOnline() {
    return this.nodeStatus.values().stream().anyMatch(ONodeDatabaseState::isOnline);
  }

  private boolean isSelfOnline() {
    var state = this.nodeStatus.get(current);
    if (state != null) {
      return state.isOnline();
    } else {
      return false;
    }
  }

  public synchronized boolean isQuorumOnline() {
    long online = this.nodeStatus.values().stream().filter(ONodeDatabaseState::isOnline).count();
    return online >= quorum;
  }

  public synchronized boolean isAllOnline() {
    return this.nodeStatus.values().stream().allMatch(ONodeDatabaseState::isOnline);
  }

  public synchronized boolean waitOnlineOne(Optional<Long> timeout) throws InterruptedException {
    return waitFor(timeout, this::isOneOnline);
  }

  public synchronized boolean waitSelfOnline(Optional<Long> timeout) throws InterruptedException {
    return waitFor(timeout, this::isSelfOnline);
  }

  public synchronized void executeOnOneOnline(ONotificationAction execute) {
    executeOn(this::isOneOnline, execute);
  }

  private void notifyChange(ONodeId node, ODatabaseState state) {
    this.stateListener.onStateChange(id, node, state);
    super.notifyChange();
  }

  public synchronized Set<ONodeId> getOnlineNodes() {
    return nodeStatus.values().stream()
        .filter((x) -> x.isOnline())
        .map((x) -> x.getId())
        .collect(Collectors.toSet());
  }

  public synchronized Optional<OSyncInfo> newSync() {
    if (!syncSessions.isEmpty()) {
      logger.debugNode(current, "Cannot create new sync, one already present %s", syncSessions);
      return Optional.empty();
    }
    Set<ONodeId> onlineNodes = new HashSet<>(getOnlineNodes());
    onlineNodes.remove(current);
    if (onlineNodes.isEmpty()) {
      logger.debugNode(current, "Cannot create new sync for db %s, no online nodes", getId());
      return Optional.empty();
    }
    OSyncSession session = new OSyncSession(getId(), current, onlineNodes);
    this.syncSessions.put(session.getSyncId(), session);
    return Optional.of(new OSyncInfo(session.getSyncId(), onlineNodes, session.getFinished()));
  }

  public synchronized Optional<OCanSyncResult> canSync(
      ONodeId sender, OSyncId syncId, OCanSyncAccept canSync) {
    OSyncSession session = this.syncSessions.get(syncId);
    if (session != null) {
      Optional<OCanSyncResult> result = session.canSync(sender, syncId, canSync);
      if (result.isEmpty() && session.isFinished()) {
        this.syncSessions.remove(syncId);
      }
      return result;
    } else {
      return Optional.empty();
    }
  }

  public synchronized Optional<OSyncState> startSend(
      ONodeId from, OSyncId syncId, OCanSyncAccept mode) {
    OSyncSession session = this.syncSessions.get(syncId);
    if (session != null) {
      return session.startSync(from, syncId, mode);
    } else {
      return Optional.empty();
    }
  }

  public synchronized boolean acceptSync(ONodeId sender, OSyncId syncId) {
    for (OSyncSession session : this.syncSessions.values()) {
      if (session.isTransferingData()) {
        return false;
      }
    }
    if (this.nodeStatus.get(sender).isOnline()) {
      OSyncSession session = new OSyncSession(getId(), syncId);
      this.syncSessions.put(syncId, session);
      return true;
    } else {
      return false;
    }
  }

  public synchronized void requestNext(OSyncId syncId, boolean close) {
    var sync = this.syncSessions.get(syncId);
    if (sync != null) {
      Optional<OSyncState> state = sync.getState();
      if (state.isPresent()) {
        state.get().requestNext(close);
      } else if (close) {
        this.syncSessions.remove(syncId);
      } else {
        logger.warnNode(
            current,
            "Received next request for %s but missing sync state, closed:%b",
            syncId,
            close);
      }
    } else {
      logger.warnNode(
          current,
          "Received next request for %s but missing sync session, closed:%b",
          syncId,
          close);
    }
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

  public synchronized void receiveState(ODatabaseStateNetwork state, boolean notify) {
    // TODO: verify promised case ....
    if (this.getVersion().isBefore(state.version())) {
      this.versionPromise.loadVersion(state.version());
      this.quorum = state.quorum();
      for (ODatabaseMemberNetwork member : state.members()) {
        ONodeDatabaseState status = this.nodeStatus.get(member.node());
        if (status != null) {
          if (status.getState() != member.state()) {
            status.setState(member.state());
            if (notify) {
              this.stateListener.onStateChange(id, member.node(), member.state());
            }
          }
          status.setRole(member.role());
        } else {
          var m = new ONodeDatabaseState(member.node(), member.role(), member.state());
          this.nodeStatus.put(member.node(), m);
          if (notify) {
            this.stateListener.onStateChange(id, member.node(), member.state());
          }
        }
      }
    }
  }

  public synchronized void mergeState(ODatabaseStateNetwork state, OTransactionIdPromise promise) {
    this.versionPromise.forceVersion(state.version());
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

  public synchronized ONodeRole getRole(ONodeId nodeId) {
    ONodeDatabaseState stat = this.nodeStatus.get(nodeId);
    if (stat != null) {
      return stat.getRole();
    }
    return null;
  }

  public synchronized Optional<OAcceptResult> promiseMember(
      List<OAddNodeInfo> nodes, OVersion version, OTransactionIdPromise promise) {
    for (var node : nodes) {
      if (this.nodeStatus.containsKey(node.node())) {
        return Optional.of(new ONodeAlreadyPresent(this.id, node.node()));
      }
    }
    return this.versionPromise.promise(promise, version);
  }

  public synchronized Optional<OAcceptResult> promiseRemoveMember(
      List<ONodeId> nodes, OVersion version, OTransactionIdPromise promise) {
    for (var node : nodes) {
      if (!this.nodeStatus.containsKey(node)) {
        return Optional.of(new OMissingNode(node));
      }
    }
    return this.versionPromise.promise(promise, version);
  }

  public synchronized void removeMember(
      List<ONodeId> nodes, OVersion version, OTransactionIdPromise promise) {
    for (var node : nodes) {
      this.nodeStatus.remove(node);
      this.stateListener.onStateChange(id, node, ODatabaseState.Offline);
    }
    this.versionPromise.accept(promise, version);
  }

  public synchronized void cancelRemoveMemer(OTransactionIdPromise promise) {
    this.versionPromise.cancel(promise);
  }

  public synchronized void addMember(
      List<OAddNodeInfo> nodes, OVersion version, OTransactionIdPromise promise) {
    for (var node : nodes) {
      this.nodeStatus.put(
          node.node(), new ONodeDatabaseState(node.node(), node.role(), ODatabaseState.Offline));
      this.stateListener.onStateChange(id, node.node(), ODatabaseState.Offline);
    }
    this.versionPromise.accept(promise, version);
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

  public synchronized void completeSync(OSyncId syncId, boolean success) {
    var session = syncSessions.remove(syncId);
    if (session != null) {
      session.complete(success);
    }
  }

  public synchronized ODatabaseTopologyStore getStore() {
    var nodes = this.nodeStatus.values().stream().map((x) -> x.toStore()).toList();
    return new ODatabaseTopologyStore(nodes, this.id, this.name, this.getVersion(), this.quorum);
  }

  public int getQuorum() {
    return quorum;
  }

  public synchronized Optional<OAcceptResult> validateDrop(
      OTransactionIdPromise promise, OVersion version) {
    if (!this.syncSessions.isEmpty()) {
      return Optional.of(new ODatabaseSynching(id));
    }
    return this.versionPromise.promise(promise, version);
  }

  public synchronized void drop(OTransactionIdPromise promise, OVersion version) {
    this.versionPromise.accept(promise, version);
  }

  public synchronized void cancelDrop(OTransactionIdPromise promise, OVersion version) {
    this.versionPromise.cancel(promise);
  }

  public synchronized Optional<OSyncState> getSyncState(OSyncId syncId) {
    OSyncSession session = this.syncSessions.get(syncId);
    if (session != null) {
      return session.getState();
    }
    return Optional.empty();
  }

  public synchronized Set<OSyncState> getSyncs() {
    var syncs = new HashSet<OSyncState>();
    for (var session : this.syncSessions.values()) {
      session.getState().ifPresent(syncs::add);
    }
    return syncs;
  }

  public synchronized void nodeDisconnected(ONodeId node) {
    var iter = this.syncSessions.entrySet().iterator();
    while (iter.hasNext()) {
      var sync = iter.next().getValue();
      if (sync.nodeDisconnected(node)) {
        iter.remove();
      }
    }
  }

  public synchronized Optional<OAcceptResult> validateMerge(
      OTransactionIdPromise promise, ODatabaseStateNetwork stateDb) {
    return this.versionPromise.promise(promise, stateDb.version().next());
  }

  public synchronized Optional<OAcceptResult> validateMergeNode(
      OTransactionIdPromise promise, ODatabaseStateNetwork stateDb) {
    return this.versionPromise.promise(promise, stateDb.version().next());
  }

  public synchronized void cancelMerge(OTransactionIdPromise promise) {
    this.versionPromise.cancel(promise);
  }

  public synchronized OVersion nextVersion() {
    return this.versionPromise.next();
  }

  public synchronized Optional<OAcceptResult> validateRole(
      ONodeId node, OVersion version, OTransactionIdPromise promise) {
    if (!this.nodeStatus.containsKey(node)) {
      return Optional.of(new OMissingNode(node));
    }
    return this.versionPromise.promise(promise, version);
  }

  public synchronized void setRole(
      ONodeId node, ONodeRole role, OVersion version, OTransactionIdPromise promise) {
    var no = this.nodeStatus.get(node);
    if (no != null) {
      no.setRole(role);
    }
    this.versionPromise.accept(promise, version);
  }

  public synchronized void cancelRole(
      ONodeId nodeId, OVersion version, OTransactionIdPromise promise) {
    this.versionPromise.cancel(promise);
  }

  public synchronized void notifyAllNodesStates() {
    for (ONodeDatabaseState state : this.nodeStatus.values()) {
      this.stateListener.onStateChange(id, state.getId(), state.getState());
    }
  }

  public synchronized Optional<OAcceptResult> validateSetQurum(
      int newQuorum, OVersion version, OTransactionIdPromise promise) {
    if (newQuorum < nodeStatus.size() / 2) {
      return Optional.of(new OQuormuTooSmall());
    } else if (newQuorum > nodeStatus.size()) {
      return Optional.of(new OQuormuTooBig());
    }
    return this.versionPromise.promise(promise, version);
  }

  public synchronized void setQuorum(
      int newQuorum, OVersion version, OTransactionIdPromise promise) {
    this.quorum = newQuorum;
    this.versionPromise.accept(promise, version);
  }

  public synchronized void cancelQuorum(OVersion version, OTransactionIdPromise promise) {
    this.versionPromise.cancel(promise);
  }

  public Set<ONodeId> getMembers() {
    return this.nodeStatus.keySet();
  }

  public synchronized void checkTimeoutSync(long timeOutSync) {
    var iter = this.syncSessions.values().iterator();
    while (iter.hasNext()) {
      var sync = iter.next();
      if (sync.checkTimeout(timeOutSync)) {
        iter.remove();
      }
    }
  }
}
