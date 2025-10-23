package com.orientechnologies.orient.distributed.db;

import com.orientechnologies.orient.distributed.context.coordination.message.OStructuralMessage;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.UUID;

public class ONextBuffer implements OStructuralMessage {

  private UUID syncId;

  public ONextBuffer(UUID syncId) {
    this.syncId = syncId;
  }

  @Override
  public void execute(OrientDBDistributed ctx) {
    ctx.nextBuffer(syncId);
  }

  @Override
  public void serialize(DataOutput out) throws IOException {
    out.writeUTF(syncId.toString());
  }

  @Override
  public short getType() {
    return 11;
  }

  public static ONextBuffer fromNetwork(DataInput input) throws IOException {
    UUID syncId = UUID.fromString(input.readUTF());
    return new ONextBuffer(syncId);
  }
}
