package com.orientechnologies.orient.distributed.context.coordination.message;

import com.orientechnologies.orient.core.transaction.OTransactionIdPromise;
import com.orientechnologies.orient.distributed.db.OrientDBDistributed;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class OMergeConfirmOp implements OStructuralMessage {

  private OTransactionIdPromise promise;

  public OMergeConfirmOp(OTransactionIdPromise promise) {
    this.promise = promise;
  }

  @Override
  public void execute(OrientDBDistributed ctx) {
    ctx.mergeToNetwork(promise);
  }

  @Override
  public void serialize(DataOutput out) throws IOException {
    promise.writeNetwork(out);
  }

  @Override
  public short getType() {
    return 14;
  }

  public static OMergeConfirmOp fromNetwork(DataInput input) throws IOException {
    var promise = OTransactionIdPromise.readNetwork(input);
    return new OMergeConfirmOp(promise);
  }

  @Override
  public String toString() {
    return "OMergeConfirmOp [promise=" + promise + "]";
  }
}
