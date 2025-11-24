package com.orientechnologies.orient.distributed.context;

import com.orientechnologies.orient.distributed.context.coordination.result.OAcceptResult;
import com.orientechnologies.orient.distributed.db.OrientDBDistributed;
import java.util.Optional;

public interface ORetryOperation {
  public Optional<OAcceptResult> execute(OrientDBDistributed context, ORetryInfo retry);
}
