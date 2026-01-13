package com.orientechnologies.orient.distributed.context.coordination.result;

import com.orientechnologies.orient.core.transaction.ODatabaseId;
import com.orientechnologies.orient.core.transaction.ONodeId;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.MessageFormat;

public record ONodeAlreadyPresent(ODatabaseId db, ONodeId node) implements OAcceptResult {

  @Override
  public void serialize(DataOutput out) throws IOException {
    db.writeNetwork(out);
    node.writeNetwork(out);
  }

  @Override
  public short getType() {
    return 10;
  }

  public static ONodeAlreadyPresent fromNetwork(DataInput input) throws IOException {
    ODatabaseId db = ODatabaseId.readNetwork(input);
    ONodeId node = ONodeId.readNetwork(input);
    return new ONodeAlreadyPresent(db, node);
  }

  @Override
  public String toString() {
    return MessageFormat.format("Node {0} already defined in {1} ", node, db);
  }
}
