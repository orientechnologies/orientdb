package com.orientechnologies.orient.distributed.context.coordination.result;

import com.orientechnologies.orient.core.transaction.ODatabaseId;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.MessageFormat;

public record ODatabaseMissing(ODatabaseId dbId) implements OAcceptResult {

  @Override
  public void serialize(DataOutput out) throws IOException {
    this.dbId.writeNetwork(out);
  }

  @Override
  public short getType() {
    return 7;
  }

  public static ODatabaseMissing fromNetwork(DataInput input) throws IOException {
    var dbId = ODatabaseId.readNetwork(input);
    return new ODatabaseMissing(dbId);
  }

  @Override
  public String toString() {
    return MessageFormat.format("Database {0} Missing", dbId);
  }
}
