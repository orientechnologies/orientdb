package com.orientechnologies.orient.distributed.context;

import com.orientechnologies.orient.core.transaction.ONodeId;
import com.orientechnologies.orient.core.transaction.OTransactionIdPromise;
import java.util.Set;

public interface OCompleteAction {

  void success(OTransactionIdPromise promise, Set<ONodeId> all);

  void failure(OTransactionIdPromise promise, Set<ONodeId> all);
}
