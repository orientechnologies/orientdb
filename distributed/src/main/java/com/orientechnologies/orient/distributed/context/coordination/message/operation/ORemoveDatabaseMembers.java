package com.orientechnologies.orient.distributed.context.coordination.message.operation;

import com.orientechnologies.orient.core.transaction.ODatabaseId;
import com.orientechnologies.orient.core.transaction.ONodeId;
import com.orientechnologies.orient.core.transaction.OTransactionIdPromise;
import com.orientechnologies.orient.distributed.context.coordination.OVersion;
import com.orientechnologies.orient.distributed.context.coordination.result.OAcceptResult;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class ORemoveDatabaseMembers implements OOperationMessage {

  private ODatabaseId database;
  private List<ONodeId> nodes;
  private OVersion version;

  public ORemoveDatabaseMembers(ODatabaseId database, List<ONodeId> nodes, OVersion version) {
    this.database = database;
    this.nodes = nodes;
    this.version = version;
  }

  @Override
  public Optional<OAcceptResult> validate(OOperationContext ctx, OTransactionIdPromise promise) {
    return ctx.getOps().validateRemoveDatabaseMembers(database, nodes, version, promise);
  }

  @Override
  public void apply(OOperationContext ctx, OTransactionIdPromise promise) {
    ctx.getOps().removeDatabaseMembers(database, nodes, version, promise);
  }

  @Override
  public void cancel(OOperationContext ctx, OTransactionIdPromise promise) {
    ctx.getOps().cancelRemoveDatabaseMembers(database, nodes, promise);
  }

  @Override
  public void serialize(DataOutput out) throws IOException {
    this.database.writeNetwork(out);
    out.writeInt(nodes.size());
    for (ONodeId node : this.nodes) {
      node.writeNetwork(out);
    }
    this.version.writeNetwork(out);
  }

  @Override
  public short getType() {
    return 7;
  }

  public static ORemoveDatabaseMembers readNetwork(DataInput input) throws IOException {
    ODatabaseId dbId = ODatabaseId.readNetwork(input);
    int nodesSize = input.readInt();
    List<ONodeId> nodes = new ArrayList<>(nodesSize);
    while (nodesSize-- > 0) {
      nodes.add(ONodeId.readNetwork(input));
    }
    OVersion version = OVersion.readNetwork(input);
    return new ORemoveDatabaseMembers(dbId, nodes, version);
  }

  public List<ONodeId> getNodes() {
    return nodes;
  }

  public ODatabaseId getDatabase() {
    return database;
  }

  public OVersion getVersion() {
    return version;
  }
}
