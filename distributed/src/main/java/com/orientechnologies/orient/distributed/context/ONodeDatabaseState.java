package com.orientechnologies.orient.distributed.context;

import com.orientechnologies.orient.core.transaction.ONodeId;

public class ONodeDatabaseState {

  private final ONodeId id;
  private ONodeRole role;
  private ODatabaseState state;

  public ONodeDatabaseState(ONodeId node, ONodeRole role, ODatabaseState state) {
    this.id = node;
    this.role = role;
    this.state = state;
  }

  public void setState(ODatabaseState state) {
    this.state = state;
  }

  public ODatabaseState getState() {
    return state;
  }

  public ONodeId getId() {
    return id;
  }

  public ONodeRole getRole() {
    return role;
  }

  public boolean isOnline() {
    return ODatabaseState.Online.equals(state);
  }
}
