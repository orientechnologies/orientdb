package com.orientechnologies.orient.distributed.context.coordination.message;

import com.orientechnologies.orient.distributed.context.coordination.sync.OSyncId;
import com.orientechnologies.orient.distributed.context.coordination.sync.OSyncMode;
import com.orientechnologies.orient.distributed.db.OrientDBDistributed;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class OSyncRequest implements OStructuralMessage {

  private final OSyncId syncId;
  private final OSyncMode mode;

  public OSyncRequest(OSyncId syncId, OSyncMode mode) {
    this.syncId = syncId;
    this.mode = mode;
  }

  @Override
  public void execute(OrientDBDistributed ctx) {
    ctx.acceptSync(syncId, mode);
  }

  @Override
  public void serialize(DataOutput out) throws IOException {
    mode.writeNetwork(out);
    syncId.writeNetwork(out);
  }

  @Override
  public short getType() {
    return 7;
  }

  public static OSyncRequest fromNetwork(DataInput input) throws IOException {
    OSyncMode mode = OSyncMode.fromNetwork(input);
    OSyncId syncId = OSyncId.readNetwork(input);

    return new OSyncRequest(syncId, mode);
  }

  public OSyncId getSyncId() {
    return syncId;
  }

  public OSyncMode getMode() {
    return mode;
  }
}
