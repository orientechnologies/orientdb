package com.orientechnologies.orient.distributed.context.coordination.message;

import com.orientechnologies.orient.distributed.context.coordination.sync.OSyncId;
import com.orientechnologies.orient.distributed.db.OrientDBDistributed;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class OSyncData implements OStructuralMessage {

  public final byte[] data;
  private final OSyncId syncId;
  private final boolean finished;
  private final long sequential;

  /**
   * The data need to be immutable, copy it if can mutate before passing
   *
   * @param data
   */
  public OSyncData(OSyncId syncId, byte[] data, long sequential, boolean finished) {
    this.syncId = syncId;
    this.data = data;
    this.finished = finished;
    this.sequential = sequential;
  }

  @Override
  public void execute(OrientDBDistributed ctx) {
    ctx.receiveSyncData(this.syncId, this.data, this.sequential, this.finished);
  }

  @Override
  public void serialize(DataOutput out) throws IOException {
    this.syncId.writeNetwork(out);
    out.writeInt(this.data.length);
    out.write(this.data);
    out.writeLong(sequential);
    out.writeBoolean(this.finished);
  }

  @Override
  public short getType() {
    return 10;
  }

  public static OSyncData fromNetwork(DataInput input) throws IOException {
    OSyncId syncId = OSyncId.readNetwork(input);
    int size = input.readInt();
    byte[] data = new byte[size];
    input.readFully(data);
    long sequential = input.readLong();
    boolean finished = input.readBoolean();
    return new OSyncData(syncId, data, sequential, finished);
  }

  public byte[] getData() {
    return data;
  }

  public OSyncId getSyncId() {
    return syncId;
  }

  public boolean isFinished() {
    return finished;
  }

  public long getSequential() {
    return sequential;
  }
}
