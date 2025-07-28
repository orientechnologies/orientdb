package com.orientechnologies.orient.distributed.context.topology;

import com.orientechnologies.orient.core.transaction.ONodeId;
import java.util.Set;

public class OTopologyManager implements OTopologyEvents {

  private OTopologyState state = OTopologyState.INITIAL;
  private Set<ONodeId> members;
  private Set<ONodeId> candidates;
  private long version = 0;
  private int minimumQuorum;
  private final OTopologyAction action;

  public OTopologyManager(int minimumQuorum, OTopologyAction action) {
    this.minimumQuorum = minimumQuorum;
    this.action = action;
  }

  @Override
  public void nodeDiscovered(ONodeId node) {
    Runnable toRun = null;
    synchronized (this) {
      if (state == OTopologyState.INITIAL) {
        addToPotential(node);
        if (canEstablish()) {
          toRun = enstablish(candidates);
        }
      } else if (!hasMember(node)) {
        toRun = newMemberAction(node);
      }
    }
    if (toRun != null) {
      toRun.run();
    }
  }

  protected boolean hasMember(ONodeId node) {
    return members.contains(node);
  }

  /** Run a two phase operation for add the member to the list of the member
   * @param version
   *
   */
  private Runnable newMemberAction(ONodeId node) {
    return () -> {
      action.send(new OAddTopologyMember(version, node));
    };
  }

  private boolean canEstablish() {
    return candidates.size() > minimumQuorum;
  }

  /** Run a two phase operation for agree the initial list of nodes participating in the network
   * @param candidates
   *
   */
  private Runnable enstablish(Set<ONodeId> candidates) {
    return () -> {
      action.enstablish(new OEnstablishTopology());
    };
  }

  private void addToPotential(ONodeId node) {
    candidates.add(node);
  }

  public synchronized long getVersion() {
    return version;
  }

  public synchronized boolean promise(long version, ONodeId node) {
    if (this.version == version) {
      // TOOD: hold and check promise
      return true;
    } else {
      return false;
    }
  }

  public synchronized void finalize(ONodeId node) {
    this.members.add(node);
  }

  public synchronized void confirm(long version) {
    this.version = version;
  }
}
