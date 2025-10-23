package com.orientechnologies.orient.distributed.db;

import com.orientechnologies.orient.core.transaction.ODatabaseId;
import com.orientechnologies.orient.core.transaction.ONodeId;
import com.orientechnologies.orient.distributed.context.coordination.message.OStructuralMessage;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.UUID;

public class OCanSync implements OStructuralMessage {
  private ONodeId from;
  private ODatabaseId dbId;
  private UUID syncId;
  private OSyncMode mode;
  private boolean canSync;

  public OCanSync(ONodeId from, ODatabaseId dbId, UUID syncId, OSyncMode mode, boolean canSync) {
    this.from = from;
    this.dbId = dbId;
    this.syncId = syncId;
    this.canSync = canSync;
  }

  @Override
  public void execute(OrientDBDistributed ctx) {
    ctx.canSync(from, dbId, syncId, canSync, mode);
  }

  @Override
  public void serialize(DataOutput out) throws IOException {
    this.from.writeNetwork(out);
    this.dbId.writeNetwork(out);
    out.writeUTF(syncId.toString());
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
    UUID syncId = UUID.fromString(input.readUTF());
    OSyncMode mode = OSyncMode.fromNetwork(input);
    boolean canSync = input.readBoolean();
    return new OCanSync(from, dbId, syncId, mode, canSync);
  }
}
