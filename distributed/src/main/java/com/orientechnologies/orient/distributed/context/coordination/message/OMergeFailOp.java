package com.orientechnologies.orient.distributed.context.coordination.message;

import com.orientechnologies.orient.core.transaction.OTransactionIdPromise;
import com.orientechnologies.orient.distributed.db.OrientDBDistributed;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class OMergeFailOp implements OStructuralMessage {

  private OTransactionIdPromise promise;

  public OMergeFailOp(OTransactionIdPromise promise) {
    this.promise = promise;
  }

  @Override
  public void execute(OrientDBDistributed ctx) {
    ctx.cancelMerge(promise);
  }

  @Override
  public void serialize(DataOutput out) throws IOException {
    promise.writeNetwork(out);
  }

  @Override
  public short getType() {
    return 15;
  }

  public static OMergeFailOp fromNetwork(DataInput input) throws IOException {
    var promise = OTransactionIdPromise.readNetwork(input);
    return new OMergeFailOp(promise);
  }

  @Override
  public String toString() {
    return "OMergeFailOp [promise=" + promise + "]";
  }
}
