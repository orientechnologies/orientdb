package com.orientechnologies.orient.distributed.context;

import com.orientechnologies.orient.core.transaction.ONodeId;
import com.orientechnologies.orient.core.transaction.OTransactionIdPromise;

public interface ODistributedResponse {

  OTransactionIdPromise getPromise();

  ONodeId getNode();
}
