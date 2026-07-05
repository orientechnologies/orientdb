package com.orientechnologies.orient.distributed.context.coordination.message.operation;

import com.orientechnologies.orient.core.transaction.ODatabaseId;
import com.orientechnologies.orient.core.transaction.ONodeId;
import com.orientechnologies.orient.core.transaction.ONodeRole;
import com.orientechnologies.orient.core.transaction.OTransactionIdPromise;
import com.orientechnologies.orient.distributed.context.coordination.OVersion;
import com.orientechnologies.orient.distributed.context.coordination.result.OAcceptResult;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Optional;

public class OSetDatabaseMemberRole implements OOperationMessage {

  private ODatabaseId db;
  private ONodeId node;
  private ONodeRole role;
  private OVersion version;

  public OSetDatabaseMemberRole(ODatabaseId db, ONodeId node, ONodeRole role, OVersion version) {
    this.db = db;
    this.node = node;
    this.role = role;
    this.version = version;
  }

  @Override
  public Optional<OAcceptResult> validate(OOperationContext ctx, OTransactionIdPromise promise) {
    return ctx.getOps()
        .validateSetDatabaseNodeRole(this.db, this.node, this.role, this.version, promise);
  }

  @Override
  public void apply(OOperationContext ctx, OTransactionIdPromise promise) {
    ctx.getOps().setDatabaseNodeRole(this.db, this.node, this.role, this.version, promise);
  }

  @Override
  public void cancel(OOperationContext ctx, OTransactionIdPromise promise) {
    ctx.getOps().cancelSetDatabaseNodeRole(this.db, this.node, this.role, this.version, promise);
  }

  @Override
  public void serialize(DataOutput out) throws IOException {
    this.db.writeNetwork(out);
    this.node.writeNetwork(out);
    this.role.writeNetwork(out);
    this.version.writeNetwork(out);
  }

  @Override
  public short getType() {
    return 11;
  }

  public static OSetDatabaseMemberRole readNetwork(DataInput input) throws IOException {
    var db = ODatabaseId.readNetwork(input);
    var node = ONodeId.readNetwork(input);
    var role = ONodeRole.readNetwork(input);
    var version = OVersion.readNetwork(input);
    return new OSetDatabaseMemberRole(db, node, role, version);
  }

  public ODatabaseId getDb() {
    return db;
  }

  public ONodeId getNode() {
    return node;
  }

  public ONodeRole getRole() {
    return role;
  }

  public OVersion getVersion() {
    return version;
  }
}
