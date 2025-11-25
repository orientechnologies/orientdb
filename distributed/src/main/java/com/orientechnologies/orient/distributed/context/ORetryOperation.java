package com.orientechnologies.orient.distributed.context;

import com.orientechnologies.orient.distributed.db.OCompleteExecution;
import com.orientechnologies.orient.distributed.db.OrientDBDistributed;

public interface ORetryOperation {
  public void execute(OrientDBDistributed context, OCompleteExecution execution);
}
