package com.orientechnologies.orient.server.distributed.impl;

import com.orientechnologies.orient.server.distributed.task.ODistributedOperationException;

public class OInvalidSequentialException extends ODistributedOperationException {

  public OInvalidSequentialException(ODistributedOperationException exception) {
    super(exception);
  }

  public OInvalidSequentialException() {
    super("invalid transaction sequential ");
  }
}
