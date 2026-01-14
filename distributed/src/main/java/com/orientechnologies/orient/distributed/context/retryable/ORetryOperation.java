package com.orientechnologies.orient.distributed.context.retryable;

import com.orientechnologies.orient.distributed.db.OCompleteExecution;
import com.orientechnologies.orient.distributed.db.OrientDBDistributed;

public interface ORetryOperation {
  void execute(OrientDBDistributed context, OCompleteExecution execution);
}
