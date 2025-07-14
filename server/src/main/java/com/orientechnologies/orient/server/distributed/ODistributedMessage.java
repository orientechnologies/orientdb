package com.orientechnologies.orient.server.distributed;

import com.orientechnologies.orient.core.tx.OTransactionIdPromise;

public interface ODistributedMessage {
  OTransactionIdPromise getPromiseId();
}
