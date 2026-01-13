package com.orientechnologies.orient.distributed.context.coordination.message.state;

import com.orientechnologies.orient.core.transaction.ONodeId;
import com.orientechnologies.orient.distributed.context.coordination.dbs.ODatabaseState;
import com.orientechnologies.orient.distributed.context.coordination.dbs.ONodeRole;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public record ODatabaseMemberNetwork(ONodeId node, ONodeRole role, ODatabaseState state) {

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
