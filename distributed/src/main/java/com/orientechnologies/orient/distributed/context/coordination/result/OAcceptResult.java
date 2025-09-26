package com.orientechnologies.orient.distributed.context.coordination.result;

import com.orientechnologies.orient.server.distributed.ODistributedException;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public interface OAcceptResult {

  boolean canRetry();

  static OAcceptResult readNetwork(DataInput input) throws IOException {
    return switch (input.readShort()) {
      case 1 -> OInvalidSequential.fromNetwork(input);
      case 2 -> OQuorumNotReached.fromNetwork(input);
      case 3 -> OAlreadyEnstablishedTopologyState.fromNetwork(input);
      case 4 -> ODatabaseNameUsed.fromNetwork(input);
      default -> throw new ODistributedException("wrong accept result message type from network");
    };
  }

  default void writeNetwork(DataOutput out) throws IOException {
    out.writeShort(getType());
    serialize(out);
  }

  void serialize(DataOutput out) throws IOException;

  short getType();
}
