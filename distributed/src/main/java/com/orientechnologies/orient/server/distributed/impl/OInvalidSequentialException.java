package com.orientechnologies.orient.server.distributed.impl;

import com.orientechnologies.orient.server.distributed.OTransactionId;
import com.orientechnologies.orient.server.distributed.task.ODistributedOperationException;

public class OInvalidSequentialException extends ODistributedOperationException {

  private OTransactionId expected;
  private OTransactionId received;

  public OInvalidSequentialException(ODistributedOperationException exception) {
    super(exception);
  }

  public OInvalidSequentialException(OTransactionId expected, OTransactionId received) {
    super("requested validation for:" + received + " current status:" + expected);
    this.expected = expected;
    this.received = received;
  }
}
