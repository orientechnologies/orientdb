package com.orientechnologies.orient.distributed.context.coordination.result;

import com.orientechnologies.orient.core.transaction.ONodeId;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.MessageFormat;

public record OAlreadyPromised(ONodeId promisedTo) implements OAcceptResult {

  @Override
  public boolean consensusRetry() {
    return true;
  }

  @Override
  public void serialize(DataOutput out) throws IOException {
    promisedTo.writeNetwork(out);
  }

  @Override
  public short getType() {
    return 6;
  }

  public static OAlreadyPromised fromNetwork(DataInput input) throws IOException {
    var promisedTo = ONodeId.readNetwork(input);
    return new OAlreadyPromised(promisedTo);
  }

  @Override
  public String toString() {
    return MessageFormat.format("Already Promised to {0}", promisedTo);
  }
}
