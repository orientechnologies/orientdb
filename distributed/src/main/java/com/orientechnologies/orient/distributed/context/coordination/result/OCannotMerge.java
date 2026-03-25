package com.orientechnologies.orient.distributed.context.coordination.result;

import com.orientechnologies.orient.distributed.context.coordination.message.state.ONodeStateNetwork;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public record OCannotMerge(ONodeStateNetwork currentState, OAcceptResult reaseon)
    implements OAcceptResult {

  public static OCannotMerge fromNetwork(DataInput input) throws IOException {
    var state = ONodeStateNetwork.fromNetwork(input);
    var reason = OAcceptResult.readNetwork(input);
    return new OCannotMerge(state, reason);
  }

  @Override
  public void serialize(DataOutput out) throws IOException {
    this.currentState.writeNetwork(out);
    this.reaseon.writeNetwork(out);
  }

  @Override
  public short getType() {
    return 14;
  }

  public boolean executeRetry() {
    // TODO maybe the reason do not allow retry ... to check.
    return true;
  }
}
