package com.orientechnologies.orient.distributed.context.coordination.message;

import com.orientechnologies.orient.distributed.context.coordination.sync.OSyncId;
import com.orientechnologies.orient.distributed.db.OrientDBDistributed;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class OStartSync implements OStructuralMessage {

  private OSyncId syncId;
  private OCanSyncAccept mode;

  public OStartSync(OSyncId syncId, OCanSyncAccept mode) {
    this.syncId = syncId;
    this.mode = mode;
  }

  @Override
  public void execute(OrientDBDistributed ctx) {
    ctx.sendDatabase(syncId, mode);
  }

  @Override
  public void serialize(DataOutput out) throws IOException {
    this.mode.writeNetwork(out);
    this.syncId.writeNetwork(out);
  }

  @Override
  public short getType() {
    return 9;
  }

  public static OStartSync fromNetwork(DataInput input) throws IOException {
    OCanSyncAccept mode = OCanSyncAccept.readNetwork(input);
    OSyncId syncId = OSyncId.readNetwork(input);
    return new OStartSync(syncId, mode);
  }

  public OCanSyncAccept getMode() {
    return mode;
  }

  public OSyncId getSyncId() {
    return syncId;
  }
}
