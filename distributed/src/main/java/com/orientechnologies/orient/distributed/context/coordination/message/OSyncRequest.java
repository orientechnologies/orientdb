package com.orientechnologies.orient.distributed.context.coordination.message;

import com.orientechnologies.orient.core.transaction.ODatabaseId;
import com.orientechnologies.orient.core.transaction.ONodeId;
import com.orientechnologies.orient.core.tx.OTransactionSequenceStatus;
import com.orientechnologies.orient.distributed.context.OSyncId;
import com.orientechnologies.orient.distributed.db.OSyncMode;
import com.orientechnologies.orient.distributed.db.OrientDBDistributed;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Optional;

public class OSyncRequest implements OStructuralMessage {

  private final ONodeId from;
  private final ODatabaseId dbId;
  private final OSyncId syncId;
  private final OSyncMode mode;
  private final Optional<OTransactionSequenceStatus> sequenceStatus;

  public OSyncRequest(
      ONodeId from,
      ODatabaseId dbId,
      OSyncId syncId,
      OSyncMode mode,
      Optional<OTransactionSequenceStatus> sequenceStatus) {
    this.from = from;
    this.dbId = dbId;
    this.syncId = syncId;
    this.mode = mode;
    this.sequenceStatus = sequenceStatus;
  }

  @Override
  public void execute(OrientDBDistributed ctx) {
    ctx.acceptSync(from, dbId, syncId, mode, sequenceStatus);
  }

  @Override
  public void serialize(DataOutput out) throws IOException {
    from.writeNetwork(out);
    dbId.writeNetwork(out);
    mode.writeNetwork(out);
    syncId.writeNetwork(out);
    if (sequenceStatus.isPresent()) {
      out.writeBoolean(true);
      sequenceStatus.get().writeNetwork(out);
    } else {
      out.writeBoolean(false);
    }
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
    boolean sequencePresent = input.readBoolean();
    Optional<OTransactionSequenceStatus> sequenceStatus;
    if (sequencePresent) {
      sequenceStatus = Optional.of(OTransactionSequenceStatus.readNetwork(input));
    } else {
      sequenceStatus = Optional.empty();
    }
    return new OSyncRequest(from, dbId, syncId, mode, sequenceStatus);
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

  public Optional<OTransactionSequenceStatus> getSequenceStatus() {
    return sequenceStatus;
  }
}
