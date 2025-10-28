package com.orientechnologies.orient.distributed.context.coordination.message;

import com.orientechnologies.orient.distributed.context.OSyncId;
import com.orientechnologies.orient.distributed.db.OrientDBDistributed;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class ONextBuffer implements OStructuralMessage {

  private final OSyncId syncId;
  private final boolean close;

  public ONextBuffer(OSyncId syncId, boolean close) {
    this.syncId = syncId;
    this.close = close;
  }

  @Override
  public void execute(OrientDBDistributed ctx) {
    ctx.nextBuffer(this.syncId, this.close);
  }

  @Override
  public void serialize(DataOutput out) throws IOException {
    this.syncId.writeNetwork(out);
    out.writeBoolean(close);
  }

  @Override
  public short getType() {
    return 11;
  }

  public static ONextBuffer fromNetwork(DataInput input) throws IOException {
    OSyncId syncId = OSyncId.readNetwork(input);
    boolean close = input.readBoolean();
    return new ONextBuffer(syncId, close);
  }

  public boolean isClose() {
    return close;
  }

  public OSyncId getSyncId() {
    return syncId;
  }
}
