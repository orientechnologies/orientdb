package com.orientechnologies.orient.distributed.impl.coordinator.network;

import static com.orientechnologies.orient.distributed.network.binary.OBinaryDistributedMessage.DISTRIBUTED_ACK_RESPONSE;

import com.orientechnologies.orient.core.db.config.ONodeIdentity;
import com.orientechnologies.orient.distributed.impl.log.OLogId;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class ONetworkAck implements ODistributedMessage {
  private OLogId logId;

  public ONetworkAck(OLogId logId) {
    this.logId = logId;
  }

  public ONetworkAck() {}

  @Override
  public void write(DataOutput output) throws IOException {
    OLogId.serialize(logId, output);
  }

  @Override
  public void read(DataInput input) throws IOException {
    logId = OLogId.deserialize(input);
  }

  @Override
  public void execute(ONodeIdentity sender, OCoordinatedExecutor executor) {
    executor.executeAck(sender, logId);
  }

  @Override
  public byte getCommand() {
    return DISTRIBUTED_ACK_RESPONSE;
  }

  public OLogId getLogId() {
    return logId;
  }
}
