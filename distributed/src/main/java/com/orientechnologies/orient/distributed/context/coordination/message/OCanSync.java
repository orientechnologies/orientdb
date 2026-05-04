package com.orientechnologies.orient.distributed.context.coordination.message;

import com.orientechnologies.orient.core.transaction.ODatabaseId;
import com.orientechnologies.orient.core.transaction.ONodeId;
import com.orientechnologies.orient.distributed.context.coordination.sync.OSyncId;
import com.orientechnologies.orient.distributed.db.OrientDBDistributed;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class OCanSync implements OStructuralMessage {
  private final ONodeId sender;
  private final ODatabaseId dbId;
  private final OSyncId syncId;
  private final OCanSyncAccept canSync;

  public OCanSync(ONodeId sender, ODatabaseId dbId, OSyncId syncId, OCanSyncAccept canSync) {
    this.sender = sender;
    this.dbId = dbId;
    this.syncId = syncId;
    this.canSync = canSync;
  }

  @Override
  public void execute(OrientDBDistributed ctx) {
    ctx.canSync(sender, dbId, syncId, canSync);
  }

  @Override
  public void serialize(DataOutput out) throws IOException {
    this.sender.writeNetwork(out);
    this.dbId.writeNetwork(out);
    this.syncId.writeNetwork(out);
    this.canSync.writeNetwork(out);
  }

  @Override
  public short getType() {
    return 8;
  }

  public static OCanSync fromNetwork(DataInput input) throws IOException {
    ONodeId from = ONodeId.readNetwork(input);
    ODatabaseId dbId = ODatabaseId.readNetwork(input);
    OSyncId syncId = OSyncId.readNetwork(input);
    var canSync = OCanSyncAccept.readNetwork(input);
    return new OCanSync(from, dbId, syncId, canSync);
  }

  public ONodeId getSender() {
    return sender;
  }

  public ODatabaseId getDbId() {
    return dbId;
  }

  public OSyncId getSyncId() {
    return syncId;
  }

  public OCanSyncAccept getCanSync() {
    return canSync;
  }
}
