package com.orientechnologies.orient.distributed.context.coordination.message.operation;

import com.orientechnologies.orient.core.transaction.OTransactionIdPromise;
import com.orientechnologies.orient.distributed.context.coordination.result.OAcceptResult;
import com.orientechnologies.orient.distributed.db.OrientDBDistributed;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Optional;

public class ODropDbMessage implements OOperationMessage {

  private String name;

  public ODropDbMessage(String name) {
    this.name = name;
  }

  public void apply(OrientDBDistributed ctx, OTransactionIdPromise promise) {
    ctx.internalDrop(name);
  }

  public Optional<OAcceptResult> validate(OrientDBDistributed ctx, OTransactionIdPromise promise) {
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
  public void cancel(OrientDBDistributed ctx, OTransactionIdPromise promise) {
    // Promise not tracked do nothing
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
