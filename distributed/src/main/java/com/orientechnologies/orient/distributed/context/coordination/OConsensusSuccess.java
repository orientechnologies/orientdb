package com.orientechnologies.orient.distributed.context.coordination;

import com.orientechnologies.orient.core.transaction.OTransactionIdPromise;
import com.orientechnologies.orient.distributed.context.coordination.message.ODistributedMessage;

public record OConsensusSuccess(
    ODistributedMessage success, OTransactionIdPromise failure, boolean missing, boolean applied) {
  public OConsensusSuccess() {
    this(null, null, false, false);
  }

  public OConsensusSuccess(boolean applied) {
    this(null, null, false, applied);
  }

  public OConsensusSuccess(ODistributedMessage success) {
    this(success, null, false, false);
  }

  public OConsensusSuccess(ODistributedMessage success, boolean missing) {
    this(success, null, missing, false);
  }

  public OConsensusSuccess(OTransactionIdPromise failure) {
    this(null, failure, false, false);
  }

  public boolean isSuccess() {
    return success != null;
  }

  public boolean isFailure() {
    return failure != null;
  }

  public boolean isFinished() {
    return success != null || applied;
  }
}
