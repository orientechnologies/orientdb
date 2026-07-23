package com.orientechnologies.orient.distributed.context.coordination;

import com.orientechnologies.orient.core.transaction.OTransactionIdPromise;
import com.orientechnologies.orient.distributed.context.coordination.message.ODistributedMessage;

public record OConsensusSuccess(
    ODistributedMessage success,
    OTransactionIdPromise otherPromised,
    boolean previousMissing,
    boolean alreadyApplied) {
  public OConsensusSuccess() {
    this(null, null, false, false);
  }

  public OConsensusSuccess(boolean alreadyApplied) {
    this(null, null, false, alreadyApplied);
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

  public boolean isPromisedToOther() {
    return otherPromised != null;
  }
}
