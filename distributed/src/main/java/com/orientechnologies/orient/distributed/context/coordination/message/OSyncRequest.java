package com.orientechnologies.orient.distributed.context.coordination.message;

import com.orientechnologies.orient.core.transaction.ODatabaseId;
import com.orientechnologies.orient.core.transaction.ONodeId;
import com.orientechnologies.orient.distributed.context.coordination.sync.OSyncId;
import com.orientechnologies.orient.distributed.context.coordination.sync.OSyncMode;
import com.orientechnologies.orient.distributed.db.OrientDBDistributed;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class OSyncRequest implements OStructuralMessage {

  private final ONodeId from;
  private final ODatabaseId dbId;
  private final OSyncId syncId;
  private final OSyncMode mode;

  public OSyncRequest(ONodeId from, ODatabaseId dbId, OSyncId syncId, OSyncMode mode) {
    this.from = from;
    this.dbId = dbId;
    this.syncId = syncId;
    this.mode = mode;
  }

  @Override
  public void execute(OrientDBDistributed ctx) {
    ctx.acceptSync(from, dbId, syncId, mode);
  }

  @Override
  public void serialize(DataOutput out) throws IOException {
    from.writeNetwork(out);
    dbId.writeNetwork(out);
    mode.writeNetwork(out);
    syncId.writeNetwork(out);
  }

  @Override
  public short getType() {
    return 7;
  }

  public static OSyncRequest fromNetwork(DataInput input) throws IOException {
    ONodeId from = ONodeId.readNetwork(input);
    ODatabaseId dbId = ODatabaseId.readNetwork(input);
    OSyncMode mode = OSyncMode.fromNetwork(input);
    OSyncId syncId = OSyncId.readNetwork(input);

    return new OSyncRequest(from, dbId, syncId, mode);
  }

  public ONodeId getFrom() {
    return from;
  }

  public ODatabaseId getDbId() {
    return dbId;
  }

  public OSyncId getSyncId() {
    return syncId;
  }

  public OSyncMode getMode() {
    return mode;
  }
}
