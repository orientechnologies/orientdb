package com.orientechnologies.orient.distributed.context.coordination.message;

import com.orientechnologies.orient.core.transaction.ONodeId;
import com.orientechnologies.orient.distributed.context.ODatabaseState;
import com.orientechnologies.orient.distributed.context.ONodeRole;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class ODatabaseMemberNetwork {

  private final ONodeId node;
  private final ONodeRole role;
  private final ODatabaseState state;

  public ODatabaseMemberNetwork(ONodeId node, ONodeRole role, ODatabaseState state) {
    super();
    this.node = node;
    this.role = role;
    this.state = state;
  }

  public ONodeId getNode() {
    return node;
  }

  public ODatabaseState getState() {
    return state;
  }

  public ONodeRole getRole() {
    return role;
  }

  public void writeNetwork(DataOutput output) throws IOException {
    this.node.writeNetwork(output);
    this.state.writeNetwork(output);
    this.role.writeNetwork(output);
  }

  public static ODatabaseMemberNetwork readNetwork(DataInput input) throws IOException {
    var node = ONodeId.readNetwork(input);
    var state = ODatabaseState.readNetwork(input);
    var role = ONodeRole.readNetwork(input);
    return new ODatabaseMemberNetwork(node, role, state);
  }
}
