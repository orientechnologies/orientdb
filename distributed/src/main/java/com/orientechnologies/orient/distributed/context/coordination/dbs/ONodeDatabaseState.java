package com.orientechnologies.orient.distributed.context.coordination.dbs;

import com.orientechnologies.orient.core.transaction.ONodeId;
import com.orientechnologies.orient.distributed.context.ODatabaseNodeStore;
import com.orientechnologies.orient.distributed.context.coordination.message.state.ODatabaseMemberNetwork;

public class ONodeDatabaseState {

  private final ONodeId id;
  private ONodeRole role;
  private ODatabaseState state;

  public ONodeDatabaseState(ONodeId node, ONodeRole role, ODatabaseState state) {
    this.id = node;
    this.role = role;
    this.state = state;
  }

  public ONodeDatabaseState(ODatabaseNodeStore store) {
    this.id = store.id();
    this.role = store.role();
    this.state = ODatabaseState.Offline;
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

  public void setRole(ONodeRole role) {
    this.role = role;
  }

  public boolean isOnline() {
    return ODatabaseState.Online.equals(state);
  }

  public boolean isMain() {
    return this.role == ONodeRole.Main;
  }

  public ODatabaseMemberNetwork getNetworkState() {
    return new ODatabaseMemberNetwork(getId(), getRole(), getState());
  }

  public ODatabaseNodeStore toStore() {
    return new ODatabaseNodeStore(id, role);
  }
}
