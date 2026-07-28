package com.orientechnologies.orient.distributed.context.coordination.message;

import com.orientechnologies.orient.core.transaction.OTransactionIdPromise;
import com.orientechnologies.orient.distributed.context.coordination.OConfirmResult;
import com.orientechnologies.orient.distributed.context.coordination.message.operation.OOperationContext;
import com.orientechnologies.orient.distributed.db.OrientDBDistributed;
import com.orientechnologies.orient.server.distributed.OLoggerDistributed;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class OConfirmOp implements OStructuralMessage {
  private static final OLoggerDistributed logger = OLoggerDistributed.logger(OConfirmOp.class);
  private OTransactionIdPromise promise;

  public OConfirmOp(OTransactionIdPromise promise) {
    this.promise = promise;
  }

  @Override
  public void execute(OrientDBDistributed ctx) {
    executeConfirm(ctx, promise);
  }

  public static void executeConfirm(OOperationContext ctx, OTransactionIdPromise promise) {
    OConfirmResult confirmResult;
    // The success of this operation may means the otherPromised of another
    // promised operation in case the current node was out of consent
    // it probably happen only once, but not locked so potentially can be multiple
    // promise otherPromised even though unlikely, looping anyway
    boolean complete;
    do {
      confirmResult = ctx.getOps().consensusSuccess(promise);
      complete = confirmResult.apply(ctx);
      if (!complete) {
        logger.debug("retry commit for %s", confirmResult);
      }
    } while (!complete);
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

  @Override
  public String toString() {
    return "OConfirmOp [promise=" + promise + "]";
  }
}
