package com.orientechnologies.orient.distributed.context.coordination.message;

import com.orientechnologies.orient.core.transaction.ODatabaseId;
import com.orientechnologies.orient.core.transaction.ONodeId;
import com.orientechnologies.orient.core.tx.OTransactionSequenceStatus;
import com.orientechnologies.orient.distributed.context.coordination.sync.OSyncId;
import com.orientechnologies.orient.distributed.context.coordination.sync.OSyncMode;
import com.orientechnologies.orient.distributed.db.OrientDBDistributed;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Optional;

public class OStartSync implements OStructuralMessage {

  private ONodeId receiver;
  private ODatabaseId dbId;
  private OSyncId syncId;
  private OSyncMode mode;
  private Optional<OTransactionSequenceStatus> sequenceStatus;

  public OStartSync(
      ONodeId receiver,
      ODatabaseId dbId,
      OSyncId syncId,
      OSyncMode mode,
      Optional<OTransactionSequenceStatus> sequenceStatus) {
    this.receiver = receiver;
    this.dbId = dbId;
    this.syncId = syncId;
    this.mode = mode;
    this.sequenceStatus = sequenceStatus;
  }

  @Override
  public void execute(OrientDBDistributed ctx) {
    ctx.sendDatabase(this.receiver, dbId, syncId, mode, sequenceStatus);
  }

  @Override
  public void serialize(DataOutput out) throws IOException {
    this.receiver.writeNetwork(out);
    this.dbId.writeNetwork(out);
    this.mode.writeNetwork(out);
    this.syncId.writeNetwork(out);
    if (this.sequenceStatus.isPresent()) {
      out.writeBoolean(true);
      this.sequenceStatus.get().writeNetwork(out);
    } else {
      out.writeBoolean(false);
    }
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
    Optional<OTransactionSequenceStatus> sequenceStatus;
    boolean sequence = input.readBoolean();
    if (sequence) {
      sequenceStatus = Optional.of(OTransactionSequenceStatus.readNetwork(input));
    } else {
      sequenceStatus = Optional.empty();
    }
    return new OStartSync(nodeId, dbId, syncId, mode, sequenceStatus);
  }

  public ODatabaseId getDbId() {
    return dbId;
  }

  public ONodeId getReceiver() {
    return receiver;
  }

  public OSyncMode getMode() {
    return mode;
  }

  public OSyncId getSyncId() {
    return syncId;
  }

  public Optional<OTransactionSequenceStatus> getSequenceStatus() {
    return sequenceStatus;
  }
}
