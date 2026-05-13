package com.orientechnologies.orient.distributed.context.coordination.message.operation;

import com.orientechnologies.orient.core.transaction.ODatabaseId;
import com.orientechnologies.orient.core.transaction.OTransactionIdPromise;
import com.orientechnologies.orient.distributed.context.coordination.OVersion;
import com.orientechnologies.orient.distributed.context.coordination.result.OAcceptResult;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Optional;

public class OSetDatabaseQuorum implements OOperationMessage {

  private ODatabaseId db;
  private int quorum;
  private OVersion version;

  public OSetDatabaseQuorum(ODatabaseId db, int quorum, OVersion version) {
    this.db = db;
    this.quorum = quorum;
    this.version = version;
  }

  @Override
  public Optional<OAcceptResult> validate(OOperationContext ctx, OTransactionIdPromise promise) {
    return ctx.getOps().validateSetDatabaseQuorum(this.db, quorum, this.version, promise);
  }

  @Override
  public void apply(OOperationContext ctx, OTransactionIdPromise promise) {
    ctx.getOps().setDatabaseQuorum(this.db, quorum, this.version, promise);
  }

  @Override
  public void cancel(OOperationContext ctx, OTransactionIdPromise promise) {
    ctx.getOps().cancelSetDatabaseQuorum(this.db, quorum, this.version, promise);
  }

  @Override
  public void serialize(DataOutput out) throws IOException {
    this.db.writeNetwork(out);
    out.writeInt(quorum);
    this.version.writeNetwork(out);
  }

  @Override
  public short getType() {
    return 10;
  }

  public static OSetDatabaseQuorum readNetwork(DataInput input) throws IOException {
    var db = ODatabaseId.readNetwork(input);
    int quorum = input.readInt();
    var version = OVersion.readNetwork(input);
    return new OSetDatabaseQuorum(db, quorum, version);
  }

  public ODatabaseId getDb() {
    return db;
  }

  public int getQuorum() {
    return quorum;
  }

  public OVersion getVersion() {
    return version;
  }
}
