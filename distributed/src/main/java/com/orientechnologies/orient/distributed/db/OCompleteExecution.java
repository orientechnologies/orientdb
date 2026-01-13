package com.orientechnologies.orient.distributed.db;

import com.orientechnologies.orient.distributed.context.coordination.action.ORetryInfo;
import com.orientechnologies.orient.distributed.context.coordination.result.OAcceptResult;
import java.util.Optional;

public interface OCompleteExecution {

  public ORetryInfo getRetryInfo();

  public void complete(Optional<OAcceptResult> result);
}
