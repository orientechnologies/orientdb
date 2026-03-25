package com.orientechnologies.orient.distributed.context.retryable;

import com.orientechnologies.orient.distributed.context.coordination.result.OAcceptResult;
import com.orientechnologies.orient.distributed.db.OCompleteExecution;
import com.orientechnologies.orient.distributed.db.OrientDBDistributed;
import java.util.Optional;

public interface ORetryOperation {
  void execute(
      OrientDBDistributed context,
      OCompleteExecution execution,
      Optional<OAcceptResult> previousResult);
}
