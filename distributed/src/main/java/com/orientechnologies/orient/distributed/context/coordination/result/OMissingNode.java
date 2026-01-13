package com.orientechnologies.orient.distributed.context.coordination.result;

import com.orientechnologies.orient.core.transaction.ONodeId;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.MessageFormat;

public record OMissingNode(ONodeId nodeId) implements OAcceptResult {

  @Override
  public boolean executeRetry() {
    return true;
  }

  public static OMissingNode fromNetwork(DataInput input) throws IOException {
    var node = ONodeId.readNetwork(input);
    return new OMissingNode(node);
  }

  @Override
  public void serialize(DataOutput out) throws IOException {
    this.nodeId.writeNetwork(out);
  }

  @Override
  public short getType() {
    return 5;
  }

  @Override
  public String toString() {
    return MessageFormat.format("Missing Node {0} ", nodeId);
  }
}
