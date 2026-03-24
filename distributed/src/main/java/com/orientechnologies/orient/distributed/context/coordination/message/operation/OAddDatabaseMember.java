package com.orientechnologies.orient.distributed.context.coordination.message.operation;

import com.orientechnologies.orient.core.transaction.ODatabaseId;
import com.orientechnologies.orient.core.transaction.OTransactionIdPromise;
import com.orientechnologies.orient.distributed.context.coordination.result.OAcceptResult;
import com.orientechnologies.orient.distributed.db.OrientDBDistributed;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class OAddDatabaseMember implements OOperationMessage {

  private final long version;
  private final ODatabaseId dbId;
  private final List<OAddNodeInfo> nodes;

  public OAddDatabaseMember(long version, ODatabaseId dbId, List<OAddNodeInfo> nodes) {
    this.version = version;
    this.dbId = dbId;
    this.nodes = nodes;
  }

  @Override
  public Optional<OAcceptResult> validate(OrientDBDistributed ctx, OTransactionIdPromise promise) {
    return ctx.getNodeState().getOps().validateAddDatabaseMember(dbId, nodes, version, promise);
  }

  @Override
  public void apply(OrientDBDistributed ctx, OTransactionIdPromise promise) {
    ctx.getNodeState().getOps().addDatabaseMember(dbId, nodes, version, promise);
  }

  @Override
  public void cancel(OrientDBDistributed ctx, OTransactionIdPromise promise) {
    ctx.getNodeState().getOps().cancelAddDatabaseMember(dbId, nodes, promise);
  }

  @Override
  public void serialize(DataOutput out) throws IOException {
    out.writeLong(version);
    this.dbId.writeNetwork(out);
    out.writeInt(nodes.size());
    for (OAddNodeInfo node : nodes) {
      node.writeNetwork(out);
    }
  }

  public static OAddDatabaseMember readNetwork(DataInput input) throws IOException {
    long version = input.readLong();
    ODatabaseId dbId = ODatabaseId.readNetwork(input);
    int size = input.readInt();
    List<OAddNodeInfo> nodes = new ArrayList<>();
    while (size-- > 0) {
      nodes.add(OAddNodeInfo.readNetwork(input));
    }

    return new OAddDatabaseMember(version, dbId, nodes);
  }

  @Override
  public short getType() {
    return 6;
  }

  public long getVersion() {
    return version;
  }

  public ODatabaseId getDbId() {
    return dbId;
  }

  public List<OAddNodeInfo> getNodes() {
    return nodes;
  }

  @Override
  public String toString() {
    return "Add member " + nodes + " to database " + dbId + ", version=" + version;
  }
}
