package com.orientechnologies.orient.distributed.db;

import com.orientechnologies.orient.distributed.context.ORetryOperation;
import com.orientechnologies.orient.distributed.context.coordination.action.ORetryInfo;
import com.orientechnologies.orient.distributed.context.coordination.result.OAcceptResult;
import com.orientechnologies.orient.server.distributed.OLoggerDistributed;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

public class OStandardCompleteExecution implements OCompleteExecution {

  private static final OLoggerDistributed logger =
      OLoggerDistributed.logger(OStandardCompleteExecution.class);

  private final ORetryOperation operation;
  private final ORetryInfo retryInfo;
  private final OrientDBDistributed context;
  private final CompletableFuture<Optional<OAcceptResult>> result;

  public OStandardCompleteExecution(
      OrientDBDistributed ctx, ORetryOperation operation, ORetryInfo retryInfo) {
    this.context = ctx;
    this.operation = operation;
    this.retryInfo = retryInfo;
    this.result = new CompletableFuture<Optional<OAcceptResult>>();
  }

  public ORetryInfo getRetryInfo() {
    return retryInfo;
  }

  @Override
  public void complete(Optional<OAcceptResult> result) {
    if (result.isPresent() && result.get().executeRetry()) {
      var delay = getRetryInfo().nextRetry();
      if (delay.isPresent()) {
        logger.info("retry whole operation for result %s delay %d", result, delay.get());
        this.context.retryExecution(operation, this, delay.get());
      } else {
        logger.debug("complete operation with %s", result);
        this.result.complete(result);
      }
    } else {
      logger.debug("complete operation with %s", result);
      this.result.complete(result);
    }
  }

  public Future<Optional<OAcceptResult>> getResult() {
    return result;
  }
}
