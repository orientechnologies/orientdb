package com.orientechnologies.orient.distributed.context.coordination.result;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.MessageFormat;

public record OOutdatedVersion(long proposed, long current) implements OAcceptResult {

  @Override
  public boolean executeRetry() {
    return true;
  }

  @Override
  public void serialize(DataOutput out) throws IOException {
    out.writeLong(current);
    out.writeLong(proposed);
  }

  @Override
  public short getType() {
    return 8;
  }

  public static OOutdatedVersion fromNetwork(DataInput input) throws IOException {
    long current = input.readLong();
    long proposed = input.readLong();
    return new OOutdatedVersion(current, proposed);
  }

  @Override
  public String toString() {
    return MessageFormat.format("Outdated Version(proposed:{0} current:{1})", proposed, current);
  }
}
