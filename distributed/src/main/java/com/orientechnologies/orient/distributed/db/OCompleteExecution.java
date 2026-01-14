package com.orientechnologies.orient.distributed.db;

import com.orientechnologies.orient.distributed.context.coordination.result.OAcceptResult;
import com.orientechnologies.orient.distributed.context.retryable.ORetryInfo;
import java.util.Optional;

public interface OCompleteExecution {

  public ORetryInfo getRetryInfo();

  public void complete(Optional<OAcceptResult> result);
}
