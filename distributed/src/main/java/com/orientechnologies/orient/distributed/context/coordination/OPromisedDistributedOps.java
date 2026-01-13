package com.orientechnologies.orient.distributed.context.coordination;

import com.orientechnologies.orient.core.transaction.ONodeId;
import com.orientechnologies.orient.core.transaction.OTransactionId;
import com.orientechnologies.orient.core.transaction.OTransactionIdPromise;
import com.orientechnologies.orient.distributed.context.coordination.message.ODistributedMessage;
import java.util.Map;
import java.util.Optional;

public interface OPromisedDistributedOps {

  void addPromised(ODistributedMessage message);

  ODistributedMessage getPromised(OTransactionIdPromise promise);

  Optional<ODistributedMessage> removePromised(OTransactionIdPromise promise);

  void addNotPromised(ODistributedMessage message);

  ODistributedMessage getNotPromised(OTransactionIdPromise promise);

  Optional<ODistributedMessage> removeNotPromised(OTransactionIdPromise promise);

  Optional<Map<OTransactionId, ODistributedMessage>> getPromised(ONodeId node);
}
