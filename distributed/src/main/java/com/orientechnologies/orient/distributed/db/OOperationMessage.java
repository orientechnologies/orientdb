package com.orientechnologies.orient.distributed.db;

import com.orientechnologies.orient.core.db.OrientDBInternal;
import com.orientechnologies.orient.distributed.context.coordination.message.OAcceptResult;
import com.orientechnologies.orient.server.distributed.ODistributedException;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Optional;

public interface OOperationMessage {

  Optional<OAcceptResult> validate(OrientDBDistributed ctx);

  void apply(OrientDBInternal ctx);

  static OOperationMessage readNetwork(DataInput input) throws IOException {
    return switch (input.readShort()) {
      case 1 -> ODropDbMessage.readNetwork(input);
      default -> throw new ODistributedException("wrong operation message type from network");
    };
  }

  default void writeNetwork(DataOutput out) throws IOException {
    out.writeShort(getType());
    serialize(out);
  }

  void serialize(DataOutput out) throws IOException;

  short getType();
}
