package com.orientechnologies.orient.server.distributed.impl;

import com.orientechnologies.orient.core.tx.OTransactionId;
import com.orientechnologies.orient.server.distributed.task.ODistributedOperationException;

public class OInvalidSequentialException extends ODistributedOperationException {

  private OTransactionId expected;

  public OInvalidSequentialException(ODistributedOperationException exception) {
    super(exception);
  }

  public OInvalidSequentialException(OTransactionId expected) {
    super("invalid sequential current status:" + expected);
    this.expected = expected;
  }
}
