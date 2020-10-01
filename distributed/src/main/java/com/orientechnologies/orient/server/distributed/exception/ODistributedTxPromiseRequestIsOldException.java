package com.orientechnologies.orient.server.distributed.exception;

import com.orientechnologies.orient.server.distributed.ODistributedException;

public class ODistributedTxPromiseRequestIsOldException extends ODistributedException {
  public ODistributedTxPromiseRequestIsOldException(String message) {
    super(message);
  }
}
