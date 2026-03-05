package com.orientechnologies.orient.distributed.context.coordination.result;

import com.orientechnologies.orient.core.transaction.ODatabaseId;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public record ODatabaseSynching(ODatabaseId dbId) implements OAcceptResult {

  public static ODatabaseSynching fromNetwork(DataInput input) throws IOException {
    var dbId = ODatabaseId.readNetwork(input);
    return new ODatabaseSynching(dbId);
  }

  @Override
  public void serialize(DataOutput out) throws IOException {
    dbId.writeNetwork(out);
  }

  @Override
  public short getType() {
    return 13;
  }

  @Override
  public boolean executeRetry() {
    return true;
  }
}
