package com.orientechnologies.orient.distributed.context.coordination.message;

import com.orientechnologies.orient.core.transaction.OTransactionIdPromise;
import com.orientechnologies.orient.distributed.db.OrientDBDistributed;

public interface ODistributedMessage {
  OTransactionIdPromise getPromiseId();

  void apply(OrientDBDistributed ctx);

  void cancel(OrientDBDistributed ctx);

  void recoordinate(OrientDBDistributed ctx);
}
