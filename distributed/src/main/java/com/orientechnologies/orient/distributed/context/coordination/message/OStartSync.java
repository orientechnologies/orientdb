package com.orientechnologies.orient.distributed.context.coordination.message;

import com.orientechnologies.orient.core.transaction.ODatabaseId;
import com.orientechnologies.orient.core.transaction.ONodeId;
import com.orientechnologies.orient.distributed.context.OSyncId;
import com.orientechnologies.orient.distributed.db.OSyncMode;
import com.orientechnologies.orient.distributed.db.OrientDBDistributed;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class OStartSync implements OStructuralMessage {

  private ONodeId from;
  private ODatabaseId dbId;
  private OSyncId syncId;
  private OSyncMode mode;

  public OStartSync(ONodeId from, ODatabaseId dbId, OSyncId syncId, OSyncMode mode) {
    this.from = from;
    this.dbId = dbId;
    this.syncId = syncId;
    this.mode = mode;
  }

  @Override
  public void execute(OrientDBDistributed ctx) {
    ctx.sendDatabase(this.from, dbId, syncId, mode);
  }

  @Override
  public void serialize(DataOutput out) throws IOException {
    this.from.writeNetwork(out);
    this.dbId.writeNetwork(out);
    this.mode.writeNetwork(out);
    this.syncId.writeNetwork(out);
  }

  @Override
  public short getType() {
    return 9;
  }

  public static OStartSync fromNetwork(DataInput input) throws IOException {
    ONodeId nodeId = ONodeId.readNetwork(input);
    ODatabaseId dbId = ODatabaseId.readNetwork(input);
    OSyncMode mode = OSyncMode.fromNetwork(input);
    OSyncId syncId = OSyncId.readNetwork(input);
    return new OStartSync(nodeId, dbId, syncId, mode);
  }

  public ODatabaseId getDbId() {
    return dbId;
  }

  public ONodeId getFrom() {
    return from;
  }

  public OSyncMode getMode() {
    return mode;
  }

  public OSyncId getSyncId() {
    return syncId;
  }
}
