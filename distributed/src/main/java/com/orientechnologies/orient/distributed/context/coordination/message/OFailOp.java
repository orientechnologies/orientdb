package com.orientechnologies.orient.distributed.context.coordination.message;

import com.orientechnologies.orient.core.transaction.OTransactionIdPromise;
import com.orientechnologies.orient.distributed.db.OrientDBDistributed;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Optional;

public class OFailOp implements OStructuralMessage {

  private OTransactionIdPromise promise;

  public OFailOp(OTransactionIdPromise promise) {
    this.promise = promise;
  }

  @Override
  public void execute(OrientDBDistributed ctx) {
    Optional<ODistributedMessage> operation = ctx.getNodeState().receiveFailure(promise);
    if (operation.isPresent()) {
      operation.get().cancel(ctx);
    }
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

  public OTransactionIdPromise getPromise() {
    return promise;
  }
}
