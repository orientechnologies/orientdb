package com.orientechnologies.orient.distributed.context.topology;

import com.orientechnologies.orient.core.transaction.ONodeId;
import com.orientechnologies.orient.distributed.context.ONodeStateStore;
import com.orientechnologies.orient.distributed.context.coordination.message.ONodeStateNetwork;
import com.orientechnologies.orient.distributed.context.coordination.result.OAcceptResult;
import com.orientechnologies.orient.distributed.context.coordination.result.OAlreadyEnstablishedTopologyState;
import com.orientechnologies.orient.distributed.context.topology.ODiscoverAction.OAddNodeAction;
import com.orientechnologies.orient.distributed.context.topology.ODiscoverAction.OEstablishAction;
import com.orientechnologies.orient.distributed.context.topology.ODiscoverAction.ONoneAction;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

public class OTopologyManager implements OTopologyEvents {

  private Optional<String> networkId = Optional.empty();
  private OTopologyState state = OTopologyState.BOOT;
  private Set<ONodeId> members = new HashSet<>();
  private Set<ONodeId> candidates = new HashSet<>();
  private volatile long version = 0;
  private volatile int minimumQuorum;
  private volatile int quorum = 0;

  public OTopologyManager(int minimumQuorum) {
    this.minimumQuorum = minimumQuorum;
  }

  @Override
  public synchronized ODiscoverAction nodeDiscovered(ONodeId node) {
    if (state == OTopologyState.BOOT) {
      addToCandidates(node);
      if (canEstablish()) {
        return new OEstablishAction(new HashSet<>(candidates));
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

  public synchronized boolean promise(ONodeId toAdd, long version) {
    if (this.version + 1 == version) {
      // TODO: hold and check promise for avoid double promise
      return true;
    } else {
      return false;
    }
  }

  public synchronized void register(ONodeId toRegister, long version) {
    // TODO: verify promise and clean it, verification is not needed is just for solidity
    if (members.add(toRegister)) {
      int newQuorum = (members.size() / 2) + 1;
      if (newQuorum >= minimumQuorum) {
        this.quorum = newQuorum;
      }
    }
    this.version = version;
  }

  public synchronized boolean enoughNodes() {
    return this.members.size() < this.minimumQuorum;
  }

  public synchronized void unregister(ONodeId node, long version) {
    if (members.remove(node)) {
      int newQuorum = (members.size() / 2) + 1;
      if (newQuorum >= minimumQuorum) {
        this.quorum = newQuorum;
      }
    }
    this.version = version;
  }

  public int getQuorum() {
    return quorum;
  }

  public int getMinimumQuorum() {
    return minimumQuorum;
  }

  public Set<ONodeId> getMembers() {
    // Would be better to have members being copy on write
    return new HashSet(members);
  }

  public synchronized void finalizeEnstablish(Set<ONodeId> candidates) {
    this.state = OTopologyState.ESTABLISHED;
    this.networkId = Optional.of(UUID.randomUUID().toString());
    this.members = candidates;
    this.candidates = new HashSet<>();
    this.version = 0;
  }

  public synchronized Optional<OAcceptResult> validateEnstablish(Set<ONodeId> candidates) {
    if (this.state == OTopologyState.BOOT) {
      return Optional.empty();
    }
    return Optional.of(new OAlreadyEnstablishedTopologyState());
  }

  public ODiscoverAction checkExternNodeState(ONodeId node, ONodeStateNetwork externState) {
    if (externState.getState() == OTopologyState.BOOT) {
      return nodeDiscovered(node);
    } else {
      synchronized (this) {
        // TODO: before applying check if any promise or running a coordination
        if (state == OTopologyState.BOOT) {
          this.state = externState.getState();
          this.members = externState.getMembers();
          this.version = externState.getVersion();
          this.networkId = externState.getNetworkId();
        } else if (this.networkId.equals(externState.getNetworkId())) {
          if (externState.getVersion() > version) {
            // TODO: check also matching network id
            this.members = externState.getMembers();
            this.version = externState.getVersion();
          } else if (externState.getVersion() != version) {
            // TODO: send local version to the sender because is outdated
          }
        } else {
          // TODO: failure crashed different networks ...
        }
      }
    }
    return new ODiscoverAction.ONoneAction();
  }

  public synchronized ONodeStateNetwork getNetworkState() {
    return new ONodeStateNetwork(this.networkId, this.state, this.members, this.version);
  }

  public void load(ONodeStateStore nodeStateStore) {
    // TODO Auto-generated method stub

  }
}
