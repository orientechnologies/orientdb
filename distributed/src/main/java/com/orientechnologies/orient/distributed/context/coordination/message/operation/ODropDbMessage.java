package com.orientechnologies.orient.distributed.context.coordination.message.operation;

import com.orientechnologies.orient.core.transaction.ODatabaseId;
import com.orientechnologies.orient.core.transaction.OTransactionIdPromise;
import com.orientechnologies.orient.distributed.context.coordination.OVersion;
import com.orientechnologies.orient.distributed.context.coordination.result.OAcceptResult;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Optional;

public class ODropDbMessage implements OOperationMessage {

  private final ODatabaseId dbId;
  private final OVersion version;

  public ODropDbMessage(ODatabaseId name, OVersion version) {
    this.dbId = name;
    this.version = version;
  }

  public void apply(OOperationContext ctx, OTransactionIdPromise promise) {
    ctx.distributedDrop(this.dbId, this.version, promise);
  }

  public Optional<OAcceptResult> validate(OOperationContext ctx, OTransactionIdPromise promise) {
    return ctx.validateDropDatabase(this.dbId, this.version, promise);
  }

  public static ODropDbMessage readNetwork(DataInput input) throws IOException {
    var id = ODatabaseId.readNetwork(input);
    var version = OVersion.readNetwork(input);
    return new ODropDbMessage(id, version);
  }

  @Override
  public void cancel(OOperationContext ctx, OTransactionIdPromise promise) {
    ctx.cancelDropDatabase(this.dbId, this.version, promise);
  }

  @Override
  public short getType() {
    return 1;
  }

  @Override
  public void serialize(DataOutput out) throws IOException {
    dbId.writeNetwork(out);
    version.writeNetwork(out);
  }

  public ODatabaseId getDbId() {
    return dbId;
  }

  public OVersion getVersion() {
    return version;
  }

  @Override
  public String toString() {
    return "Dropping database with " + dbId + "";
  }
}
