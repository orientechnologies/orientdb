package com.orientechnologies.orient.distributed.context.coordination;

import com.orientechnologies.orient.core.transaction.OTransactionIdPromise;
import com.orientechnologies.orient.distributed.context.coordination.message.ODistributedMessage;
import com.orientechnologies.orient.distributed.context.coordination.message.operation.OOperationContext;
import com.orientechnologies.orient.server.distributed.OLoggerDistributed;

public sealed interface OConfirmResult
    permits OConfirmResult.Success,
        OConfirmResult.SuccessNotPromised,
        OConfirmResult.PromisedToOther,
        OConfirmResult.AlreadyPresent,
        OConfirmResult.MissingPrevious {
  public record Success(ODistributedMessage toApply) implements OConfirmResult {
    @Override
    public boolean apply(OOperationContext ctx) {
      if (toApply != null) {
        toApply.apply(ctx);
        ctx.apllied(toApply.getPromiseId());
      } else {
        // if missing can't execute just skipping and relay on sync
      }
      return true;
    }
  }

  public record SuccessNotPromised(ODistributedMessage toApply) implements OConfirmResult {
    private static final OLoggerDistributed logger =
        OLoggerDistributed.logger(SuccessNotPromised.class);

    @Override
    public boolean apply(OOperationContext ctx) {
      if (toApply != null) {
        var prom = toApply.validate(ctx);
        // If there is an error
        if (prom.isEmpty()) {
          toApply.apply(ctx);
          ctx.apllied(toApply.getPromiseId());
        } else {
          // Can't do much, just end it here and wait for sync
          logger.warn("Error on re-validating confirmed operation: %s ", prom.get());
        }
      } else {
        // if missing can't execute just skipping and relay on sync
      }
      return true;
    }
  }

  public record PromisedToOther(OTransactionIdPromise toCancel) implements OConfirmResult {
    private static final OLoggerDistributed logger =
        OLoggerDistributed.logger(PromisedToOther.class);

    @Override
    public boolean apply(OOperationContext ctx) {
      var cancelled = ctx.getOps().consensusFailure(toCancel);
      if (cancelled.isPresent()) {
        cancelled.get().cancel(ctx);
      } else {
        logger.warn("Error cannot cancel failed missing operation");
      }
      return false;
    }
  }

  public record AlreadyPresent() implements OConfirmResult {
    @Override
    public boolean apply(OOperationContext ctx) {
      return true;
    }
  }

  public record MissingPrevious() implements OConfirmResult {
    @Override
    public boolean apply(OOperationContext ctx) {
      // Mark it as complete ?? but it need to recover actually
      return true;
    }
  }

  public static OConfirmResult success(ODistributedMessage success) {
    return new Success(success);
  }

  public static OConfirmResult successNotPromised(ODistributedMessage success) {
    return new SuccessNotPromised(success);
  }

  public static OConfirmResult promisedToOther(OTransactionIdPromise otherToCancel) {
    return new PromisedToOther(otherToCancel);
  }

  public static OConfirmResult alreadyPresent() {
    return new AlreadyPresent();
  }

  public static OConfirmResult missingPrevious() {
    return new MissingPrevious();
  }

  /** Applied the operation based on the kind of result,
   *
   * @param ctx
   * @return if return true means is complete, false means it should retry.
   */
  public boolean apply(OOperationContext ctx);
}
