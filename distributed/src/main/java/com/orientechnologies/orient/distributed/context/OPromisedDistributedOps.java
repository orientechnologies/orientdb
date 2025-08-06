package com.orientechnologies.orient.distributed.context;

import com.orientechnologies.orient.core.transaction.OTransactionIdPromise;
import com.orientechnologies.orient.distributed.context.coordination.message.ODistributedMessage;

public interface OPromisedDistributedOps {

  void add(ODistributedMessage message);

  ODistributedMessage get(OTransactionIdPromise promise);

  void remove(OTransactionIdPromise promise);
}
