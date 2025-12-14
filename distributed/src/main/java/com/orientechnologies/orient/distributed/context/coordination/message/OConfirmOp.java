package com.orientechnologies.orient.distributed.context.coordination.message;

import com.orientechnologies.orient.core.transaction.OTransactionIdPromise;
import com.orientechnologies.orient.distributed.db.OrientDBDistributed;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Optional;

public class OConfirmOp implements OStructuralMessage {
  private OTransactionIdPromise promise;

  public OConfirmOp(OTransactionIdPromise promise) {
    this.promise = promise;
  }

  @Override
  public void execute(OrientDBDistributed ctx) {
    Optional<ODistributedMessage> message = ctx.getNodeState().receiveSuccess(promise);
    if (message.isPresent()) {
      message.get().apply(ctx);
      ctx.getNodeState().complete(promise);
    } else {
      // TODO: maybe here request to sync/resend promised, or just wait for message to arrive
    }
  }

  @Override
  public void serialize(DataOutput out) throws IOException {
    promise.writeNetwork(out);
  }

  public static OConfirmOp fromNetwork(DataInput input) throws IOException {
    OTransactionIdPromise promise = OTransactionIdPromise.readNetwork(input);
    return new OConfirmOp(promise);
  }

  @Override
  public short getType() {
    return 4;
  }

  public OTransactionIdPromise getPromise() {
    return promise;
  }
}
