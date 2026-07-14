package com.orientechnologies.orient.distributed.context.coordination;

import com.orientechnologies.orient.core.transaction.OTransactionIdPromise;
import com.orientechnologies.orient.distributed.context.coordination.message.ODistributedMessage;

public record OConsensusSuccess(
    ODistributedMessage success, OTransactionIdPromise failure, boolean missing) {
  public OConsensusSuccess() {
    this(null, null, false);
  }

  public OConsensusSuccess(ODistributedMessage success) {
    this(success, null, false);
  }

  public OConsensusSuccess(ODistributedMessage success, boolean missing) {
    this(success, null, missing);
  }

  public OConsensusSuccess(OTransactionIdPromise failure) {
    this(null, failure, false);
  }

  public boolean isSuccess() {
    return success != null;
  }

  public boolean isFailure() {
    return failure != null;
  }

  public boolean isFinished() {
    return success != null;
  }
}
