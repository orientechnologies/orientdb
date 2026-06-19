package com.orientechnologies.orient.server.distributed;

import com.orientechnologies.orient.core.transaction.ONodeId;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public record ONodeMessages(ONodeId node, long messages) {

  public void writeNetwork(DataOutput out) throws IOException {
    node.writeNetwork(out);
    out.writeLong(messages);
  }

  public static ONodeMessages readNetwork(DataInput input) throws IOException {
    var node = ONodeId.readNetwork(input);
    var messages = input.readLong();
    return new ONodeMessages(node, messages);
  }
}
