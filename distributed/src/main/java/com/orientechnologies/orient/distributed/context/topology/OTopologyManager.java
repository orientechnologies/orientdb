package com.orientechnologies.orient.distributed.context.topology;

import java.util.Set;

import com.orientechnologies.orient.core.transaction.ONodeId;

public class OTopologyManager implements OTopologyEvents {

  private OTopologyState state = OTopologyState.INITIAL;
  private Set<ONodeId> members;
  private Set<ONodeId> candidates;
  private long version=0;
  private int minimumQuorum;
  public OTopologyManager(int minimumQuorum) {
    this.minimumQuorum =minimumQuorum;
  }

  @Override
  public void nodeDiscovered(ONodeId node) {
    if (state == OTopologyState.INITIAL) {
      addToPotential(node);
      enstablishIfPossible();
    } else if (!members.contains(node)) {
      tryAddNewMember(node);
    }
  }

  /** Run a two phase operation for add the member to the list of the member
   * if the quorum allow it
   */
  private void tryAddNewMember(ONodeId node) {}

  private void enstablishIfPossible() {
    if (candidates.size() > minimumQuorum) {
      enstablish();
    }
  }

  /** Run a two phase operation for agree the initial list of nodes participating in the network
   *
   */
  private void enstablish() {}

  private void addToPotential(ONodeId node) {
    candidates.add(node);
  }
}
