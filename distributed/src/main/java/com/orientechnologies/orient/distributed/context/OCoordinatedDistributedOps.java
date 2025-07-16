package com.orientechnologies.orient.distributed.context;

import com.orientechnologies.orient.core.transaction.ONodeId;
import com.orientechnologies.orient.core.transaction.OTransactionIdPromise;
import java.util.Set;

public interface OCoordinatedDistributedOps {

  Set<ONodeId> start(OTransactionIdPromise promise, OCompleteAction action);

  void success(ONodeId node, OTransactionIdPromise promise);

  void unregisterNode(ONodeId node);

  void registerNode(ONodeId node);

  Set<ONodeId> getActiveNodes();

  void failure(ONodeId node, OTransactionIdPromise promise);
}
