package com.orientechnologies.orient.distributed.db;

import com.orientechnologies.orient.core.transaction.OTransactionIdPromise;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class OFailOp implements OStructuralMessage {

  private OTransactionIdPromise promise;

  public OFailOp(OTransactionIdPromise promise) {
    this.promise = promise;
  }

  @Override
  public void execute(OrientDBDistributed ctx) {
    ctx.getNodeState().receiveFailure(promise);
  }

  @Override
  public void serialize(DataOutput out) throws IOException {
    promise.writeNetwork(out);
  }

  public static OFailOp fromNetwork(DataInput input) throws IOException {
    OTransactionIdPromise promise = OTransactionIdPromise.readNetwork(input);
    return new OFailOp(promise);
  }

  @Override
  public short getType() {
    return 5;
  }
}
