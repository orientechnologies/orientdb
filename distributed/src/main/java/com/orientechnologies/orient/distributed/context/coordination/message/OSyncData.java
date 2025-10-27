package com.orientechnologies.orient.distributed.context.coordination.message;

import com.orientechnologies.orient.distributed.context.OSyncId;
import com.orientechnologies.orient.distributed.db.OrientDBDistributed;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class OSyncData implements OStructuralMessage {

  public final byte[] data;
  private OSyncId syncId;

  /**
   * The data need to be immutable, copy it if can mutate before passing
   *
   * @param data
   */
  public OSyncData(OSyncId syncId, byte[] data) {
    this.syncId = syncId;
    this.data = data;
  }

  @Override
  public void execute(OrientDBDistributed ctx) {
    ctx.receiveSyncData(this.syncId, this.data);
  }

  @Override
  public void serialize(DataOutput out) throws IOException {
    this.syncId.writeNetwork(out);
    out.writeInt(this.data.length);
    out.write(this.data);
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
    return new OSyncData(syncId, data);
  }

  public byte[] getData() {
    return data;
  }

  public OSyncId getSyncId() {
    return syncId;
  }
}
