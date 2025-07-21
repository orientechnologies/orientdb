package com.orientechnologies.orient.distributed.context.coordination.message;

import com.orientechnologies.orient.distributed.db.OrientDBDistributed;
import com.orientechnologies.orient.server.distributed.ODistributedException;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public interface OStructuralMessage {

  void execute(OrientDBDistributed ctx);

  void serialize(DataOutput out) throws IOException;

  static OStructuralMessage readNetwork(DataInput input) throws IOException {
    return switch (input.readShort()) {
      case 1 -> OProposeOp.fromNetwork(input);
      case 2 -> OSuccessPropose.fromNetwork(input);
      case 3 -> OFailPropose.fromNetwork(input);
      case 4 -> OConfirmOp.fromNetwork(input);
      case 5 -> OFailOp.fromNetwork(input);
      default -> throw new ODistributedException("wrong structural message type from network");
    };
  }

  default void writeNetwork(DataOutput output) throws IOException {
    output.writeShort(getType());
    this.serialize(output);
  }

  short getType();
}
