package com.orientechnologies.orient.distributed.context.coordination.message.operation;

import com.orientechnologies.orient.core.transaction.OTransactionIdPromise;
import com.orientechnologies.orient.distributed.context.coordination.result.OAcceptResult;
import com.orientechnologies.orient.distributed.db.OrientDBDistributed;
import com.orientechnologies.orient.server.distributed.ODistributedException;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Optional;

public interface OOperationMessage {

  Optional<OAcceptResult> validate(OrientDBDistributed ctx, OTransactionIdPromise promise);

  void apply(OrientDBDistributed ctx, OTransactionIdPromise promise);

  void cancel(OrientDBDistributed ctx, OTransactionIdPromise promise);

  static OOperationMessage readNetwork(DataInput input) throws IOException {
    return switch (input.readShort()) {
      case 1 -> ODropDbMessage.readNetwork(input);
      case 2 -> OAddTopologyMember.readNetwork(input);
      case 3 -> OEstablishTopology.readNetwork(input);
      case 4 -> ODeclareDbMessage.readNetwork(input);
      case 5 -> OSetDatabaseState.readNetwork(input);
      case 6 -> OAddDatabaseMember.readNetwork(input);
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
