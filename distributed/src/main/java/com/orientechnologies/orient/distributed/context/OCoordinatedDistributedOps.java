package com.orientechnologies.orient.distributed.context;

import com.orientechnologies.orient.core.transaction.OGroupId;
import com.orientechnologies.orient.core.transaction.ONodeId;
import com.orientechnologies.orient.core.transaction.OTransactionIdPromise;
import com.orientechnologies.orient.distributed.context.coordination.message.OTopologyStateNetwork;
import com.orientechnologies.orient.distributed.context.coordination.result.OAcceptResult;
import com.orientechnologies.orient.distributed.context.topology.ODiscoverAction;
import java.util.Optional;
import java.util.Set;

public interface OCoordinatedDistributedOps {

  // Methods for coordinations of structural operations not of nodes

  OOperationStart start(OTransactionIdPromise promise, OCompleteAction action);

  void success(ONodeId node, OTransactionIdPromise promise);

  void failure(ONodeId node, OTransactionIdPromise promise, OAcceptResult acceptResult);

  void completeExecution(OTransactionIdPromise promise);

  // Methods for coordinations of operations to add and remove nodes

  ODiscoverAction discoverNode(ONodeId node);

  ODiscoverAction nodeJoinStart(ONodeId node, OTopologyStateNetwork state);

  Optional<OAcceptResult> promiseRegister(ONodeId node, long version);

  void registerNode(ONodeId node, long version);

  void unregisterNode(ONodeId node, long version);

  // Methods for coordinations of  operations to add establish the first network of nodes

  void startEstablish(OTransactionIdPromise idPromise, Set<ONodeId> nodes, OCompleteAction action);

  Optional<OAcceptResult> validateEnstablish(OGroupId networkId, Set<ONodeId> candidates);

  Set<ONodeId> enstablish(OGroupId networkId, Set<ONodeId> candidates);

  Set<ONodeId> getMembers();

  OTopologyStateNetwork getNetworkState();

  void load(ONodeStateStore nodeStateStore);

  void cancelRegisterPromise();

  void cancelEnstablish();
}
