package com.orientechnologies.orient.server.distributed;

import com.orientechnologies.orient.core.db.OrientDBInternal;
import com.orientechnologies.orient.core.transaction.OTransactionIdPromise;

public interface ODistributedMessage {
  OTransactionIdPromise getPromiseId();

  void apply(OrientDBInternal ctx);
}
