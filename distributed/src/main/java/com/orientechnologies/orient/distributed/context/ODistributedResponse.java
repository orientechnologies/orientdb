package com.orientechnologies.orient.distributed.context;

import com.orientechnologies.orient.core.id.ONodeId;
import com.orientechnologies.orient.core.transaction.OTransactionIdPromise;

public interface ODistributedResponse {

  OTransactionIdPromise getPromise();

  ONodeId getNode();
}
