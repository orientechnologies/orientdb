package com.orientechnologies.orient.distributed.db;

import com.orientechnologies.orient.core.db.OrientDBInternal;
import com.orientechnologies.orient.distributed.context.coordination.message.OAcceptResult;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Optional;

public class ODropDbMessage implements OOperationMessage {

  private String name;

  public ODropDbMessage(String name) {
    this.name = name;
  }

  public void apply(OrientDBInternal ctx) {
    ctx.internalDrop(name);
  }

  public Optional<OAcceptResult> validate(OrientDBDistributed ctx) {
    boolean result = ctx.exists(name, null, null);
    if (!result) {
      // Send Error message
    }
    return Optional.empty();
  }

  public static ODropDbMessage readNetwork(DataInput input) throws IOException {
    String name = input.readUTF();
    return new ODropDbMessage(name);
  }

  @Override
  public short getType() {
    return 1;
  }

  @Override
  public void serialize(DataOutput out) throws IOException {
    out.writeUTF(name);
  }

  public String getName() {
    return name;
  }
}
