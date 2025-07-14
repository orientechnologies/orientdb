package com.orientechnologies.orient.distributed.db;

import com.orientechnologies.orient.core.transaction.OTransactionIdPromise;
import com.orientechnologies.orient.server.distributed.ODistributedMessage;

public interface OPromisedDistributedOps {

  void add(ODistributedMessage message);

  ODistributedMessage get(OTransactionIdPromise promise);

  void remove(OTransactionIdPromise promise);
}
