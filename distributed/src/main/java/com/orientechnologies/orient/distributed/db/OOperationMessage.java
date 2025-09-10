package com.orientechnologies.orient.distributed.db;

import com.orientechnologies.orient.distributed.context.coordination.result.OAcceptResult;
import com.orientechnologies.orient.distributed.context.topology.OAddTopologyMember;
import com.orientechnologies.orient.distributed.context.topology.OEnstablishTopology;
import com.orientechnologies.orient.server.distributed.ODistributedException;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Optional;

public interface OOperationMessage {

  Optional<OAcceptResult> validate(OrientDBDistributed ctx);

  void apply(OrientDBDistributed ctx);

  void cancel(OrientDBDistributed ctx);

  static OOperationMessage readNetwork(DataInput input) throws IOException {
    return switch (input.readShort()) {
      case 1 -> ODropDbMessage.readNetwork(input);
      case 2 -> OAddTopologyMember.readNetwork(input);
      case 3 -> OEnstablishTopology.readNetwork(input);
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
