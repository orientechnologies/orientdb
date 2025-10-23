package com.orientechnologies.orient.distributed.db;

import com.orientechnologies.orient.distributed.context.coordination.message.OStructuralMessage;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.UUID;

public class OSyncData implements OStructuralMessage {

  public final byte[] data;
  private UUID syncId;

  /**
   * The data need to be immutable, copy it if can mutate before passing
   *
   * @param data
   */
  public OSyncData(UUID syncId, byte[] data) {
    this.syncId = syncId;
    this.data = data;
  }

  @Override
  public void execute(OrientDBDistributed ctx) {
    ctx.receiveSyncData(this.syncId, this.data);
  }

  @Override
  public void serialize(DataOutput out) throws IOException {
    out.writeInt(data.length);
    out.write(data);
  }

  @Override
  public short getType() {
    return 10;
  }

  public static OSyncData fromNetwork(DataInput input) throws IOException {
    UUID syncId = UUID.fromString(input.readUTF());
    int size = input.readInt();
    byte[] data = new byte[size];
    input.readFully(data);
    return new OSyncData(syncId, data);
  }
}
