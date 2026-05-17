package com.orientechnologies.orient.distributed.context.coordination.message.operation;

import com.orientechnologies.orient.core.transaction.OTransactionIdPromise;
import com.orientechnologies.orient.distributed.context.coordination.OVersion;
import com.orientechnologies.orient.distributed.context.coordination.result.OAcceptResult;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Optional;

public class OSetTopologyQuorum implements OOperationMessage {

  private int quorum;
  private OVersion version;

  public OSetTopologyQuorum(int quorum, OVersion version) {
    this.quorum = quorum;
    this.version = version;
  }

  @Override
  public Optional<OAcceptResult> validate(OOperationContext ctx, OTransactionIdPromise promise) {
    return ctx.getOps().validateSetTopologyQuorum(quorum, this.version, promise);
  }

  @Override
  public void apply(OOperationContext ctx, OTransactionIdPromise promise) {
    ctx.getOps().setTopologyQuorum(quorum, this.version, promise);
  }

  @Override
  public void cancel(OOperationContext ctx, OTransactionIdPromise promise) {
    ctx.getOps().cancelSetTopologyQuorum(quorum, this.version, promise);
  }

  @Override
  public void serialize(DataOutput out) throws IOException {
    out.writeInt(quorum);
    this.version.writeNetwork(out);
  }

  @Override
  public short getType() {
    return 10;
  }

  public static OSetTopologyQuorum readNetwork(DataInput input) throws IOException {
    int quorum = input.readInt();
    var version = OVersion.readNetwork(input);
    return new OSetTopologyQuorum(quorum, version);
  }

  public int getQuorum() {
    return quorum;
  }

  public OVersion getVersion() {
    return version;
  }
}
