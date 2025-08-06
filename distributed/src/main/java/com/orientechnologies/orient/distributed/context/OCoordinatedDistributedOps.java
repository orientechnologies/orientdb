package com.orientechnologies.orient.distributed.context;

import com.orientechnologies.orient.core.transaction.ONodeId;
import com.orientechnologies.orient.core.transaction.OTransactionId;
import com.orientechnologies.orient.core.transaction.OTransactionIdPromise;
import com.orientechnologies.orient.distributed.context.coordination.result.OAcceptResult;
import com.orientechnologies.orient.distributed.context.topology.ODiscoverAction;
import com.orientechnologies.orient.distributed.context.topology.StartEnstablish;
import java.util.Optional;
import java.util.Set;

public interface OCoordinatedDistributedOps {

  OOperationStart start(OTransactionIdPromise promise, OCompleteAction action);

  void success(ONodeId node, OTransactionIdPromise promise);

  void failure(ONodeId node, OTransactionIdPromise promise, OAcceptResult acceptResult);

  void unregisterNode(ONodeId node, long version);

  void registerNode(ONodeId node, long version);

  ODiscoverAction discoverNode(ONodeId node);

  Set<ONodeId> getMembers();

  void completeExecution(OTransactionId complete);

  boolean promiseRegister(ONodeId node, long version);

  void enstablish(Set<ONodeId> candidates);

  Optional<OAcceptResult> validateEnstablish(Set<ONodeId> candidates);

  StartEnstablish startEnstablish(OTransactionIdPromise idPromise, OCompleteAction action);

  long getTopologyVersion();
}
