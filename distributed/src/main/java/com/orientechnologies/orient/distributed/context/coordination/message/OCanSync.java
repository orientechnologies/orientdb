package com.orientechnologies.orient.distributed.context.coordination.message;

import com.orientechnologies.orient.core.transaction.ODatabaseId;
import com.orientechnologies.orient.core.transaction.ONodeId;
import com.orientechnologies.orient.distributed.context.OSyncId;
import com.orientechnologies.orient.distributed.db.OSyncMode;
import com.orientechnologies.orient.distributed.db.OrientDBDistributed;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class OCanSync implements OStructuralMessage {
  private ONodeId sender;
  private ODatabaseId dbId;
  private OSyncId syncId;
  private OSyncMode mode;
  private boolean canSync;

  public OCanSync(
      ONodeId sender, ODatabaseId dbId, OSyncId syncId, OSyncMode mode, boolean canSync) {
    this.sender = sender;
    this.dbId = dbId;
    this.syncId = syncId;
    this.mode = mode;
    this.canSync = canSync;
  }

  @Override
  public void execute(OrientDBDistributed ctx) {
    ctx.canSync(sender, dbId, syncId, canSync, mode);
  }

  @Override
  public void serialize(DataOutput out) throws IOException {
    this.sender.writeNetwork(out);
    this.dbId.writeNetwork(out);
    this.syncId.writeNetwork(out);
    this.mode.writeNetwork(out);
    out.writeBoolean(canSync);
  }

  @Override
  public short getType() {
    return 8;
  }

  public static OCanSync fromNetwork(DataInput input) throws IOException {
    ONodeId from = ONodeId.readNetwork(input);
    ODatabaseId dbId = ODatabaseId.readNetwork(input);
    OSyncId syncId = OSyncId.readNetwork(input);
    OSyncMode mode = OSyncMode.fromNetwork(input);
    boolean canSync = input.readBoolean();
    return new OCanSync(from, dbId, syncId, mode, canSync);
  }

  public ONodeId getSender() {
    return sender;
  }

  public ODatabaseId getDbId() {
    return dbId;
  }

  public OSyncMode getMode() {
    return mode;
  }

  public OSyncId getSyncId() {
    return syncId;
  }

  public boolean isCanSync() {
    return canSync;
  }
}
