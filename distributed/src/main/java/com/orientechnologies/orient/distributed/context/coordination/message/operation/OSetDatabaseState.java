package com.orientechnologies.orient.distributed.context.coordination.message.operation;

import com.orientechnologies.orient.core.transaction.ODatabaseId;
import com.orientechnologies.orient.core.transaction.ONodeId;
import com.orientechnologies.orient.core.transaction.OTransactionIdPromise;
import com.orientechnologies.orient.distributed.context.coordination.dbs.ODatabaseState;
import com.orientechnologies.orient.distributed.context.coordination.result.OAcceptResult;
import com.orientechnologies.orient.distributed.db.OrientDBDistributed;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Optional;

public class OSetDatabaseState implements OOperationMessage {

  private final ODatabaseId dbId;
  private final ONodeId nodeId;
  private final ODatabaseState state;
  private final long version;

  public OSetDatabaseState(ODatabaseId dbId, ONodeId nodeId, ODatabaseState state, long version) {
    this.dbId = dbId;
    this.nodeId = nodeId;
    this.state = state;
    this.version = version;
  }

  @Override
  public Optional<OAcceptResult> validate(OrientDBDistributed ctx, OTransactionIdPromise promise) {
    return ctx.getNodeState()
        .getOps()
        .validateSetState(this.dbId, this.nodeId, this.state, this.version);
  }

  @Override
  public void apply(OrientDBDistributed ctx, OTransactionIdPromise promise) {
    ctx.getNodeState().getOps().setState(this.dbId, this.nodeId, this.state, this.version);
  }

  @Override
  public void cancel(OrientDBDistributed ctx, OTransactionIdPromise promise) {
    ctx.getNodeState().getOps().cancelSetState(dbId, nodeId, version);
  }

  @Override
  public void serialize(DataOutput out) throws IOException {
    this.dbId.writeNetwork(out);
    this.nodeId.writeNetwork(out);
    this.state.writeNetwork(out);
    out.writeLong(version);
  }

  public static OSetDatabaseState readNetwork(DataInput input) throws IOException {
    ODatabaseId id = ODatabaseId.readNetwork(input);
    ONodeId nodeId = ONodeId.readNetwork(input);
    ODatabaseState state = ODatabaseState.readNetwork(input);
    long version = input.readLong();
    return new OSetDatabaseState(id, nodeId, state, version);
  }

  @Override
  public short getType() {
    return 5;
  }

  public ODatabaseId getDbId() {
    return dbId;
  }

  public ONodeId getNodeId() {
    return nodeId;
  }

  public ODatabaseState getState() {
    return state;
  }

  public long getVersion() {
    return version;
  }

  @Override
  public String toString() {
    return "Set database "
        + dbId
        + " state for "
        + nodeId
        + " to "
        + state
        + " version="
        + version
        + "";
  }
}
