package com.orientechnologies.orient.distributed.context;

import com.orientechnologies.orient.core.transaction.ONodeId;
import com.orientechnologies.orient.core.transaction.OTransactionId;
import com.orientechnologies.orient.core.transaction.OTransactionIdPromise;
import com.orientechnologies.orient.distributed.context.coordination.result.OAcceptResult;
import java.util.Set;

public interface OCoordinatedDistributedOps {

  OOperationStart start(OTransactionIdPromise promise, OCompleteAction action);

  void success(ONodeId node, OTransactionIdPromise promise);

  void failure(ONodeId node, OTransactionIdPromise promise, OAcceptResult acceptResult);

  void unregisterNode(ONodeId node);

  void registerNode(ONodeId node);

  Set<ONodeId> getActiveNodes();

  void completeExecution(OTransactionId complete);
}
