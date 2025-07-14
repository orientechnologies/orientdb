package com.orientechnologies.orient.server.distributed;

import com.orientechnologies.orient.core.transaction.OTransactionIdPromise;

public interface ODistributedMessage {
  OTransactionIdPromise getPromiseId();
}
