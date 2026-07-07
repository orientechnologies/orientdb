package com.orientechnologies.orient.distributed.context.coordination.message.operation;

import com.orientechnologies.orient.core.transaction.ONodeId;
import com.orientechnologies.orient.distributed.context.coordination.dbs.ONodeRole;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public record OAddNodeInfo(ONodeId node, ONodeRole role) {

  public static OAddNodeInfo main(ONodeId node) {
    return new OAddNodeInfo(node, ONodeRole.Main);
  }

  public void writeNetwork(DataOutput out) throws IOException {
    node.writeNetwork(out);
    role.writeNetwork(out);
  }

  public static OAddNodeInfo readNetwork(DataInput input) throws IOException {
    var node = ONodeId.readNetwork(input);
    var role = ONodeRole.readNetwork(input);
    return new OAddNodeInfo(node, role);
  }
}
