package com.orientechnologies.orient.distributed.context.coordination.message;

import com.orientechnologies.orient.core.transaction.OTransactionIdPromise;
import com.orientechnologies.orient.distributed.context.coordination.OConsensusSuccess;
import com.orientechnologies.orient.distributed.db.OrientDBDistributed;
import com.orientechnologies.orient.server.distributed.OLoggerDistributed;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class OConfirmOp implements OStructuralMessage {
  private OLoggerDistributed logger = OLoggerDistributed.logger(OConfirmOp.class);
  private OTransactionIdPromise promise;

  public OConfirmOp(OTransactionIdPromise promise) {
    this.promise = promise;
  }

  @Override
  public void execute(OrientDBDistributed ctx) {
    OConsensusSuccess successResult;
    // The success of this operation may means the failure of another
    // promised operation in case the current node was out of consent
    // it probably happen only once, but not locked so potentially can be multiple
    // promise failure even though unlikely, looping anyway
    do {
      successResult = ctx.getOps().consensusSuccess(promise);
      if (successResult.isFailure()) {
        var cancelled = ctx.getOps().consensusFailure(successResult.failure());
        if (cancelled.isPresent()) {
          cancelled.get().cancel(ctx);
        }
      } else if (successResult.isSuccess()) {
        try {
          if (successResult.missing()) {
            var prom = successResult.success().validate(ctx);
            assert prom.isEmpty();
          }
          successResult.success().apply(ctx);
        } catch (Exception e) {
          // TOOD: do something about this.
          logger.error("Error on apply operation %s need recover", e, successResult.success());
        }
        ctx.getNodeState().complete(promise);
      } else {
        // TODO: maybe here request to sync/resend promised, or just wait for message to arrive
      }
    } while (!successResult.isFinished());
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
