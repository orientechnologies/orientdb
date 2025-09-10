package com.orientechnologies.orient.distributed.context;

import com.orientechnologies.orient.core.transaction.OTransactionIdPromise;
import com.orientechnologies.orient.distributed.context.coordination.message.ODistributedMessage;
import java.util.Optional;

public interface OPromisedDistributedOps {

  void add(ODistributedMessage message);

  ODistributedMessage get(OTransactionIdPromise promise);

  Optional<ODistributedMessage> remove(OTransactionIdPromise promise);
}
