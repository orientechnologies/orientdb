package com.orientechnologies.orient.distributed.context.coordination;

import com.orientechnologies.orient.core.transaction.OTransactionIdPromise;
import com.orientechnologies.orient.distributed.context.coordination.message.ODistributedMessage;

public record OConsensusSuccess(ODistributedMessage success, OTransactionIdPromise failure) {
  public OConsensusSuccess() {
    this(null, null);
  }

  public OConsensusSuccess(ODistributedMessage success) {
    this(success, null);
  }

  public OConsensusSuccess(OTransactionIdPromise failure) {
    this(null, failure);
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
