package com.orientechnologies.orient.distributed.context.coordination.topology;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.log.OLogger;
import com.orientechnologies.orient.core.transaction.OGroupId;
import com.orientechnologies.orient.core.transaction.ONodeId;
import com.orientechnologies.orient.core.transaction.OTransactionIdPromise;
import com.orientechnologies.orient.distributed.context.ONetworkTopologyStore;
import com.orientechnologies.orient.distributed.context.coordination.OVersion;
import com.orientechnologies.orient.distributed.context.coordination.OVersionPromise;
import com.orientechnologies.orient.distributed.context.coordination.message.state.OTopologyStateNetwork;
import com.orientechnologies.orient.distributed.context.coordination.result.OAcceptResult;
import com.orientechnologies.orient.distributed.context.coordination.result.OAlreadyEstablishedTopologyState;
import com.orientechnologies.orient.distributed.context.coordination.result.ONotQuorumOneMerge;
import com.orientechnologies.orient.distributed.context.coordination.topology.ODiscoverAction.OAddNodeAction;
import com.orientechnologies.orient.distributed.context.coordination.topology.ODiscoverAction.OEstablishAction;
import com.orientechnologies.orient.distributed.context.coordination.topology.ODiscoverAction.OMergeNodeAction;
import com.orientechnologies.orient.distributed.context.coordination.topology.ODiscoverAction.ONoneAction;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public class OTopologyManager implements OTopologyEvents {
  private static final OLogger logger = OLogManager.instance().logger(OTopologyManager.class);

  private final ONodeId current;
  private OGroupId groupId;
  private OTopologyState state = OTopologyState.BOOT;
  private Set<ONodeId> members = Collections.unmodifiableSet(new HashSet<>());
  private Set<ONodeId> candidates = new HashSet<>();
  private volatile int minimumQuorum;
  private volatile int quorum = 0;
  private final OVersionPromise versionPromise;
  private Map<ONodeId, ONodeInfo> nodesInfo = new HashMap<>();

  public OTopologyManager(ONodeId current, OGroupId groupId, int minimumQuorum) {
    this.current = current;
    this.groupId = groupId;
    this.minimumQuorum = minimumQuorum;
    this.versionPromise = new OVersionPromise(new OVersion(0), current);
  }

  @Override
  public synchronized ODiscoverAction nodeDiscovered(ONodeId node) {
    if (state == OTopologyState.BOOT) {
      addToCandidates(node);
      if (canEstablish()) {
        return new OEstablishAction(groupId, new HashSet<>(candidates));
      }
    } else if (!hasMember(node)) {
      return new OAddNodeAction(node);
    }
    return new ONoneAction();
  }

  protected boolean hasMember(ONodeId node) {
    return members.contains(node);
  }

  private boolean canEstablish() {
    return candidates.size() >= minimumQuorum;
  }

  private synchronized void addToCandidates(ONodeId node) {
    candidates.add(node);
  }

  public long getVersion() {
    return versionPromise.getVersion().getValue();
  }

  public synchronized Optional<OAcceptResult> promiseRegister(
      ONodeId toAdd, long version, OTransactionIdPromise promise) {
    return this.versionPromise.promise(promise, new OVersion(version));
  }

  public synchronized void register(
      ONodeId toRegister, long version, OTransactionIdPromise promise) {
    // TODO: verify promise and clean it, verification is not needed is just for solidity
    if (!members.contains(toRegister)) {
      var newMenbers = new HashSet<ONodeId>(members);
      newMenbers.add(toRegister);
      this.members = Collections.unmodifiableSet(newMenbers);
      int newQuorum = (members.size() / 2) + 1;
      if (newQuorum >= minimumQuorum) {
        this.quorum = newQuorum;
      }
      this.nodesInfo.put(toRegister, new ONodeInfo());
    }
    this.versionPromise.accept(promise, new OVersion(version));
  }

  public synchronized boolean enoughNodes() {
    return this.members.size() < this.minimumQuorum;
  }

  public synchronized void unregister(
      ONodeId node, OVersion version, OTransactionIdPromise promise) {
    if (members.contains(node)) {
      var newMenbers = new HashSet<ONodeId>(members);
      newMenbers.remove(node);
      this.members = Collections.unmodifiableSet(newMenbers);
      int newQuorum = (members.size() / 2) + 1;
      if (newQuorum >= minimumQuorum) {
        this.quorum = newQuorum;
      }
      this.nodesInfo.remove(node);
    }
    this.versionPromise.accept(promise, version);
  }

  public synchronized int getQuorum() {
    if (this.state == OTopologyState.ESTABLISHED) {
      return quorum;
    } else {
      return getMinimumQuorum();
    }
  }

  public synchronized int getMinimumQuorum() {
    return minimumQuorum;
  }

  public synchronized Set<ONodeId> getMembers() {
    return members;
  }

  public synchronized Set<ONodeId> finalizeEstablish(
      OGroupId groupId, Set<ONodeId> candidates, OTransactionIdPromise promise) {
    assert this.groupId.equals(groupId);
    this.state = OTopologyState.ESTABLISHED;
    setMember(candidates);
    this.quorum = (members.size() / 2) + 1;
    Set<ONodeId> allNodes = new HashSet<>(candidates);
    allNodes.addAll(this.candidates);
    this.candidates = new HashSet<>();
    this.versionPromise.accept(promise, new OVersion(1));
    return allNodes;
  }

  private void setMember(Set<ONodeId> members) {
    this.members = Collections.unmodifiableSet(new HashSet<ONodeId>(members));
    logger.debug("new network members %s ", this.members);
    Map<ONodeId, ONodeInfo> newNodesInfo = new HashMap<>();
    for (var member : members) {
      var info = this.nodesInfo.get(member);
      if (info == null) {
        info = new ONodeInfo();
      }
      newNodesInfo.put(member, info);
    }
    this.nodesInfo = newNodesInfo;
  }

  public synchronized Optional<OAcceptResult> validateEstablish(
      OGroupId groupId, Set<ONodeId> candidates, OTransactionIdPromise promise) {
    if (this.state == OTopologyState.BOOT) {
      return this.versionPromise.promise(promise, new OVersion(1));
    }
    return Optional.of(new OAlreadyEstablishedTopologyState());
  }

  public ODiscoverAction nodeJoinStart(
      ONodeId node, OTopologyStateNetwork externState, boolean merge) {
    if (!this.groupId.equals(externState.groupId())) {
      // Different network ... for now ignore .. maybe crash, for sure warn;
      return new ODiscoverAction.ONoneAction();
    }
    if (externState.state() == OTopologyState.BOOT) {
      return nodeDiscovered(node);
    } else {
      synchronized (this) {
        // TODO: before applying check if any promise or running a coordination
        if (externState.members().contains(current)) {
          if (state == OTopologyState.BOOT) {
            this.state = externState.state();
            this.setMember(externState.members());
            this.versionPromise.loadVersion(new OVersion(externState.version()));
            this.quorum = externState.quorum();
            return new ODiscoverAction.OApplyStateAction();
          } else if (this.quorum == 1 && this.members.size() == 1) {
            this.setMember(externState.members());
            this.versionPromise.forceVersion(new OVersion(externState.version()));
            this.quorum = externState.quorum();
            return new ODiscoverAction.OApplySequenceAction();
          } else if (externState.version() > getVersion()) {
            this.setMember(externState.members());
            this.versionPromise.loadVersion(new OVersion(externState.version()));
            this.quorum = externState.quorum();
            return new ODiscoverAction.OApplyStateAction();
          } else if (externState.version() != getVersion()) {
            // Other outdated just notify self state
            return new ODiscoverAction.ONotifySelf(Set.of(node));
          }
        } else if (merge && !members.contains(node)) {
          return new OMergeNodeAction(node);
        } else if (this.quorum == 1) {
          /// Try to merge the state if possible
          return new ODiscoverAction.ORequestMergeAction(node);
        } else {
          // TODO: Optimize this notifying only missing members
          return new ODiscoverAction.ONotifySelf(externState.members());
        }
      }
    }
    return new ODiscoverAction.ONoneAction();
  }

  public synchronized OTopologyStateNetwork getNetworkState() {
    return new OTopologyStateNetwork(
        this.groupId, this.state, this.members, this.quorum, getVersion());
  }

  public synchronized void load(ONetworkTopologyStore nodeStateStore) {
    this.groupId = nodeStateStore.getGroupId();
    this.state = nodeStateStore.getState();
    this.versionPromise.loadVersion(new OVersion(nodeStateStore.getVersion()));
    this.quorum = nodeStateStore.getQuorum();
    this.setMember(nodeStateStore.getMembers());
  }

  public synchronized ONetworkTopologyStore getStore() {
    return new ONetworkTopologyStore(groupId, state, this.members, quorum, getVersion());
  }

  public synchronized void cancelRegisterPromise(OTransactionIdPromise promise) {
    this.versionPromise.cancel(promise);
  }

  public synchronized void cancelUnregisterPromise(OTransactionIdPromise promise) {
    this.versionPromise.cancel(promise);
  }

  public synchronized void cancelEstablish(OTransactionIdPromise promise) {
    this.versionPromise.cancel(promise);
  }

  public OGroupId getGroupId() {
    return groupId;
  }

  public ONodeId getNodeId() {
    return current;
  }

  public long nextVersion() {
    return getVersion() + 1;
  }

  public synchronized Optional<OAcceptResult> acceptMerge(
      OGroupId group, OTransactionIdPromise promise) {
    if (this.quorum == 1) {
      // This is going to merge not based on version hack the accept version
      var nextVersion = this.versionPromise.next();
      return this.versionPromise.promise(promise, nextVersion);
    } else {
      return Optional.of(new ONotQuorumOneMerge());
    }
  }

  public synchronized void cancelMerge(OTransactionIdPromise promise) {
    this.versionPromise.cancel(promise);
  }

  public synchronized void ping(ONodeId node) {
    var info = nodesInfo.get(node);
    if (info != null) {
      info.ping();
    } else {
      logger.warn("received ping for not registered node %s", node);
    }
  }

  public synchronized Set<ONodeId> awayNodes(long time) {
    Set<ONodeId> nodes = new HashSet<>();
    for (var entry : nodesInfo.entrySet()) {
      if (entry.getValue().awayMoreThan(time)) {
        nodes.add(entry.getKey());
      }
    }
    return nodes;
  }

  public Optional<OAcceptResult> promiseUnregister(
      ONodeId node, OVersion version, OTransactionIdPromise promise) {
    return this.versionPromise.promise(promise, version);
  }
}
