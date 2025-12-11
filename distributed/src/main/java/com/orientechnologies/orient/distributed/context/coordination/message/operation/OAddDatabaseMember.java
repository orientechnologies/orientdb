package com.orientechnologies.orient.distributed.context.coordination.message.operation;

import com.orientechnologies.orient.core.transaction.ODatabaseId;
import com.orientechnologies.orient.core.transaction.ONodeId;
import com.orientechnologies.orient.core.transaction.OTransactionIdPromise;
import com.orientechnologies.orient.distributed.context.ONodeRole;
import com.orientechnologies.orient.distributed.context.coordination.result.OAcceptResult;
import com.orientechnologies.orient.distributed.db.OrientDBDistributed;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Optional;

public class OAddDatabaseMember implements OOperationMessage {

  private final long version;
  private final ODatabaseId dbId;
  private final ONodeId node;
  private final ONodeRole role;

  public OAddDatabaseMember(long version, ONodeId node, ODatabaseId dbId, ONodeRole role) {
    this.version = version;
    this.dbId = dbId;
    this.node = node;
    this.role = role;
  }

  @Override
  public Optional<OAcceptResult> validate(OrientDBDistributed ctx, OTransactionIdPromise promise) {
    return ctx.getNodeState().promiseAddDatabaseMember(dbId, node, version);
  }

  @Override
  public void apply(OrientDBDistributed ctx, OTransactionIdPromise promise) {
    ctx.getNodeState().addDatabaseMember(dbId, node, role, version);
  }

  @Override
  public void cancel(OrientDBDistributed ctx, OTransactionIdPromise promise) {
    ctx.getNodeState().cancelAddDatabaseMember(dbId, node);
  }

  @Override
  public void serialize(DataOutput out) throws IOException {
    out.writeLong(version);
    this.dbId.writeNetwork(out);
    this.node.writeNetwork(out);
    this.role.writeNetwork(out);
  }

  public static OAddDatabaseMember readNetwork(DataInput input) throws IOException {
    long version = input.readLong();
    ODatabaseId dbId = ODatabaseId.readNetwork(input);
    ONodeId node = ONodeId.readNetwork(input);
    ONodeRole role = ONodeRole.readNetwork(input);
    return new OAddDatabaseMember(version, node, dbId, role);
  }

  @Override
  public short getType() {
    return 6;
  }

  public ONodeId getNode() {
    return node;
  }

  public long getVersion() {
    return version;
  }

  public ODatabaseId getDbId() {
    return dbId;
  }

  public ONodeRole getRole() {
    return role;
  }
}
