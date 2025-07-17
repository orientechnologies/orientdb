package com.orientechnologies.orient.distributed.db;

import com.orientechnologies.orient.core.db.OrientDBInternal;
import com.orientechnologies.orient.core.transaction.OTransactionIdPromise;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Optional;

public class ODropDbMessage implements OOperationMessage {

  private String name;

  public ODropDbMessage(OTransactionIdPromise promise, String name) {
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

  @Override
  public void writeNetwork(DataOutput out) throws IOException {
    out.writeUTF(name);
  }
}
