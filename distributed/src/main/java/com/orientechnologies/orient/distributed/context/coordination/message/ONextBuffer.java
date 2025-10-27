package com.orientechnologies.orient.distributed.context.coordination.message;

import com.orientechnologies.orient.distributed.context.OSyncId;
import com.orientechnologies.orient.distributed.db.OrientDBDistributed;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class ONextBuffer implements OStructuralMessage {

  private OSyncId syncId;

  public ONextBuffer(OSyncId syncId) {
    this.syncId = syncId;
  }

  @Override
  public void execute(OrientDBDistributed ctx) {
    ctx.nextBuffer(syncId);
  }

  @Override
  public void serialize(DataOutput out) throws IOException {
    this.syncId.writeNetwork(out);
  }

  @Override
  public short getType() {
    return 11;
  }

  public static ONextBuffer fromNetwork(DataInput input) throws IOException {
    OSyncId syncId = OSyncId.readNetwork(input);
    return new ONextBuffer(syncId);
  }

  public OSyncId getSyncId() {
    return syncId;
  }
}
