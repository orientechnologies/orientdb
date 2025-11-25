package com.orientechnologies.orient.distributed.db;

import com.orientechnologies.orient.distributed.context.ORetryInfo;
import com.orientechnologies.orient.distributed.context.ORetryOperation;
import com.orientechnologies.orient.distributed.context.coordination.result.OAcceptResult;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

public class OStandardCompleteExecution implements OCompleteExecution {

  private ORetryOperation operation;
  private ORetryInfo retryInfo;
  private OrientDBDistributed context;
  private CompletableFuture<Optional<OAcceptResult>> result;

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
        this.context.retryExecution(operation, this, delay.get());
      } else {
        this.result.complete(result);
      }
    } else {
      this.result.complete(result);
    }
  }

  public Future<Optional<OAcceptResult>> getResult() {
    return result;
  }
}
