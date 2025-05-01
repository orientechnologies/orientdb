package com.orientechnologies.orient.distributed;

import java.util.Set;

public class OTopologyManager implements OTopologyEvents {

  private OTopologyState state = OTopologyState.INITIAL;
  private Set<ONodeInfo> members;
  private Set<ONodeInfo> candidates;
  private long version;
  private int quorum;

  @Override
  public void nodeDiscovered(ONodeInfo node) {
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
  private void tryAddNewMember(ONodeInfo node) {}

  private void enstablishIfPossible() {
    if (candidates.size() > quorum) {
      enstablish();
    }
  }

  /** Run a two phase operation for agree the initial list of nodes participating in the network
   *
   */
  private void enstablish() {}

  private void addToPotential(ONodeInfo node) {
    candidates.add(node);
  }
}
