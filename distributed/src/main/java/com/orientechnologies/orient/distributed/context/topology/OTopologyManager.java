package com.orientechnologies.orient.distributed.context.topology;

import com.orientechnologies.orient.core.transaction.OGroupId;
import com.orientechnologies.orient.core.transaction.ONodeId;
import com.orientechnologies.orient.distributed.context.ONodeStateStore;
import com.orientechnologies.orient.distributed.context.coordination.message.OTopologyStateNetwork;
import com.orientechnologies.orient.distributed.context.coordination.result.OAcceptResult;
import com.orientechnologies.orient.distributed.context.coordination.result.OAlreadyEnstablishedTopologyState;
import com.orientechnologies.orient.distributed.context.coordination.result.OAlreadyPromised;
import com.orientechnologies.orient.distributed.context.coordination.result.OInvalidSequential;
import com.orientechnologies.orient.distributed.context.topology.ODiscoverAction.OAddNodeAction;
import com.orientechnologies.orient.distributed.context.topology.ODiscoverAction.OEstablishAction;
import com.orientechnologies.orient.distributed.context.topology.ODiscoverAction.ONoneAction;
import java.util.Collections;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

public class OTopologyManager implements OTopologyEvents {

  private final ONodeId current;
  private OGroupId groupId;
  private OTopologyState state = OTopologyState.BOOT;
  private Set<ONodeId> members = Collections.unmodifiableSet(new HashSet<>());
  private Set<ONodeId> candidates = new HashSet<>();
  private volatile long version = 0;
  private volatile int minimumQuorum;
  private volatile int quorum = 0;
  private volatile boolean promise = false;

  public OTopologyManager(ONodeId current, OGroupId groupId, int minimumQuorum) {
    this.current = current;
    this.groupId = groupId;
    this.minimumQuorum = minimumQuorum;
  }

  @Override
  public synchronized ODiscoverAction nodeDiscovered(ONodeId node) {
    if (state == OTopologyState.BOOT) {
      addToCandidates(node);
      if (canEstablish()) {
        return new OEstablishAction(groupId, new HashSet<>(candidates));
      }
    } else if (!hasMember(node)) {
      return new OAddNodeAction(node, version + 1);
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
    return version;
  }

  public synchronized Optional<OAcceptResult> promiseRegister(ONodeId toAdd, long version) {
    if (this.promise) {
      return Optional.of(new OAlreadyPromised());
    }
    if (this.version + 1 == version) {
      // TODO: maybe keep the version of promise
      this.promise = true;
      return Optional.empty();
    } else {
      return Optional.of(new OInvalidSequential(this.version + 1, version));
    }
  }

  public synchronized void register(ONodeId toRegister, long version) {
    // TODO: verify promise and clean it, verification is not needed is just for solidity
    if (!members.contains(toRegister)) {
      var newMenbers = new HashSet<ONodeId>(members);
      newMenbers.add(toRegister);
      this.members = Collections.unmodifiableSet(newMenbers);
      int newQuorum = (members.size() / 2) + 1;
      if (newQuorum >= minimumQuorum) {
        this.quorum = newQuorum;
      }
    }
    this.version = version;
    this.promise = false;
  }

  public synchronized boolean enoughNodes() {
    return this.members.size() < this.minimumQuorum;
  }

  public synchronized void unregister(ONodeId node, long version) {
    if (members.contains(node)) {
      var newMenbers = new HashSet<ONodeId>(members);
      newMenbers.remove(node);
      this.members = Collections.unmodifiableSet(newMenbers);
      int newQuorum = (members.size() / 2) + 1;
      if (newQuorum >= minimumQuorum) {
        this.quorum = newQuorum;
      }
    }
    this.version = version;
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

  public synchronized Set<ONodeId> finalizeEnstablish(OGroupId groupId, Set<ONodeId> candidates) {
    assert this.groupId.equals(groupId);
    this.state = OTopologyState.ESTABLISHED;
    setMember(candidates);
    this.quorum = (members.size() / 2) + 1;
    Set<ONodeId> allNodes = new HashSet<>(candidates);
    allNodes.addAll(this.candidates);
    this.candidates = new HashSet<>();
    this.version = 1;
    this.promise = false;
    return allNodes;
  }

  private void setMember(Set<ONodeId> members) {
    this.members = Collections.unmodifiableSet(new HashSet<ONodeId>(members));
  }

  public synchronized Optional<OAcceptResult> validateEnstablish(
      OGroupId groupId, Set<ONodeId> candidates) {
    if (this.promise) {
      return Optional.of(new OAlreadyPromised());
    }
    if (this.state == OTopologyState.BOOT) {
      promise = true;
      return Optional.empty();
    }
    return Optional.of(new OAlreadyEnstablishedTopologyState());
  }

  public ODiscoverAction nodeJoinStart(ONodeId node, OTopologyStateNetwork externState) {
    if (!this.groupId.equals(externState.getGroupId())) {
      // Different network ... for now ignore .. maybe crash, for sure warn;
      return new ODiscoverAction.ONoneAction();
    }
    if (externState.getState() == OTopologyState.BOOT) {
      return nodeDiscovered(node);
    } else {
      synchronized (this) {
        // TODO: before applying check if any promise or running a coordination
        if (externState.getMembers().contains(current)) {
          if (state == OTopologyState.BOOT) {
            this.state = externState.getState();
            this.setMember(externState.getMembers());
            this.version = externState.getVersion();
            this.quorum = externState.getQuorum();
          } else if (externState.getVersion() > version) {
            this.setMember(externState.getMembers());
            this.version = externState.getVersion();
            this.quorum = externState.getQuorum();
          } else if (externState.getVersion() != version) {
            // Other outdated just notify self state
            return new ODiscoverAction.ONotifySelf(Set.of(node));
          }
        } else if (externState.isMerge() && !members.contains(node)) {
          return new OAddNodeAction(node, version + 1);
        } else if (this.quorum == 1) {
          /// Try to merge the state if possible
          return new ODiscoverAction.OMergeAction(externState.getMembers());
        } else {
          return new ODiscoverAction.ONotifySelf(externState.getMembers());
        }
      }
    }
    return new ODiscoverAction.ONoneAction();
  }

  public synchronized OTopologyStateNetwork getNetworkState() {
    return new OTopologyStateNetwork(
        this.groupId, this.state, this.members, this.quorum, this.version);
  }

  public synchronized void load(ONodeStateStore nodeStateStore) {
    this.groupId = nodeStateStore.getGroupId();
    this.state = nodeStateStore.getState();
    this.version = nodeStateStore.getVersion();
    this.quorum = nodeStateStore.getQuorum();
    this.setMember(nodeStateStore.getMembers());
  }

  public synchronized void cancelRegisterPromise() {
    this.promise = false;
  }

  public synchronized void cancelEnstablish() {
    this.promise = false;
  }

  public OGroupId getGroupId() {
    return groupId;
  }
}
