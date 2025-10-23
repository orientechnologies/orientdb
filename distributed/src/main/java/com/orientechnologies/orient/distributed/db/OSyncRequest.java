package com.orientechnologies.orient.distributed.db;

import com.orientechnologies.orient.core.transaction.ODatabaseId;
import com.orientechnologies.orient.core.transaction.ONodeId;
import com.orientechnologies.orient.distributed.context.coordination.message.OStructuralMessage;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.UUID;

public class OSyncRequest implements OStructuralMessage {

  private ONodeId from;
  private ODatabaseId dbId;
  private UUID syncId;
  private OSyncMode mode;

  public OSyncRequest(ONodeId from, ODatabaseId dbId, UUID syncId, OSyncMode mode) {
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
    out.writeUTF(syncId.toString());
  }

  @Override
  public short getType() {
    return 7;
  }

  public static OSyncRequest fromNetwork(DataInput input) throws IOException {
    ONodeId from = ONodeId.readNetwork(input);
    ODatabaseId dbId = ODatabaseId.readNetwork(input);
    UUID syncId = UUID.fromString(input.readUTF());
    OSyncMode mode = OSyncMode.fromNetwork(input);
    return new OSyncRequest(from, dbId, syncId, mode);
  }
}
