package com.orientechnologies.orient.distributed.db;

import com.orientechnologies.orient.core.transaction.OTransactionIdPromise;
import com.orientechnologies.orient.distributed.context.ODatabaseId;
import com.orientechnologies.orient.distributed.context.coordination.result.OAcceptResult;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Optional;

public class ODeclareDbMessage implements OOperationMessage {

  private String database;
  private String uuid;

  public ODeclareDbMessage(String name, String uuid) {
    this.database = name;
    this.uuid = uuid;
  }

  @Override
  public Optional<OAcceptResult> validate(OrientDBDistributed ctx, OTransactionIdPromise promise) {
    return ctx.promiseDeclare(promise, new ODatabaseId(uuid), database);
  }

  @Override
  public void apply(OrientDBDistributed ctx, OTransactionIdPromise promise) {}

  @Override
  public void cancel(OrientDBDistributed ctx, OTransactionIdPromise promise) {}

  @Override
  public void serialize(DataOutput out) throws IOException {
    out.writeUTF(database);
    out.writeUTF(uuid);
  }

  @Override
  public short getType() {
    return 4;
  }

  public static ODeclareDbMessage readNetwork(DataInput input) throws IOException {
    String database = input.readUTF();
    String uuid = input.readUTF();
    return new ODeclareDbMessage(database, uuid);
  }
}
