package com.orientechnologies.orient.distributed.impl.coordinator.network;

import static com.orientechnologies.orient.distributed.network.binary.OBinaryDistributedMessage.DISTRIBUTED_CONFIRM_REQUEST;

import com.orientechnologies.orient.core.db.config.ONodeIdentity;
import com.orientechnologies.orient.distributed.impl.log.OLogId;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class ONetworkConfirm implements ODistributedMessage {
  private OLogId id;

  public ONetworkConfirm(OLogId id) {
    this.id = id;
  }

  public ONetworkConfirm() {}

  @Override
  public void write(DataOutput output) throws IOException {
    OLogId.serialize(id, output);
  }

  @Override
  public void read(DataInput input) throws IOException {
    id = OLogId.deserialize(input);
  }

  @Override
  public void execute(ONodeIdentity sender, OCoordinatedExecutor executor) {
    executor.executeConfirm(sender, id);
  }

  @Override
  public byte getCommand() {
    return DISTRIBUTED_CONFIRM_REQUEST;
  }

  public OLogId getId() {
    return id;
  }
}
