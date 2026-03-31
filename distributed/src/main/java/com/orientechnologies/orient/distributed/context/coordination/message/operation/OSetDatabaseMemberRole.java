package com.orientechnologies.orient.distributed.context.coordination.message.operation;

import com.orientechnologies.orient.core.transaction.ODatabaseId;
import com.orientechnologies.orient.core.transaction.ONodeId;
import com.orientechnologies.orient.core.transaction.OTransactionIdPromise;
import com.orientechnologies.orient.distributed.context.coordination.OVersion;
import com.orientechnologies.orient.distributed.context.coordination.dbs.ONodeRole;
import com.orientechnologies.orient.distributed.context.coordination.result.OAcceptResult;
import com.orientechnologies.orient.distributed.db.OrientDBDistributed;
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
  public Optional<OAcceptResult> validate(OrientDBDistributed ctx, OTransactionIdPromise promise) {
    return ctx.getNodeState()
        .getOps()
        .validateSetDatabaseNodeRole(this.db, this.node, this.role, this.version, promise);
  }

  @Override
  public void apply(OrientDBDistributed ctx, OTransactionIdPromise promise) {
    ctx.getNodeState()
        .getOps()
        .setDatabaseNodeRole(this.db, this.node, this.role, this.version, promise);
  }

  @Override
  public void cancel(OrientDBDistributed ctx, OTransactionIdPromise promise) {
    ctx.getNodeState()
        .getOps()
        .cancelSetDatabaseNodeRole(this.db, this.node, this.role, this.version, promise);
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
    return 10;
  }

  public static OSetDatabaseMemberRole readNetwork(DataInput input) throws IOException {
    var db = ODatabaseId.readNetwork(input);
    var node = ONodeId.readNetwork(input);
    var role = ONodeRole.readNetwork(input);
    var version = OVersion.readNetwork(input);
    return new OSetDatabaseMemberRole(db, node, role, version);
  }
}
