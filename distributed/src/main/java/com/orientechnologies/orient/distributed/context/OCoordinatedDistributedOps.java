package com.orientechnologies.orient.distributed.context;

import com.orientechnologies.orient.core.transaction.ODatabaseId;
import com.orientechnologies.orient.core.transaction.OGroupId;
import com.orientechnologies.orient.core.transaction.ONodeId;
import com.orientechnologies.orient.core.transaction.OTransactionIdPromise;
import com.orientechnologies.orient.core.tx.OTransactionSequenceStatus;
import com.orientechnologies.orient.distributed.context.coordination.message.ODistributedMessage;
import com.orientechnologies.orient.distributed.context.coordination.message.ONodeStateNetwork;
import com.orientechnologies.orient.distributed.context.coordination.message.operation.OAddNodeInfo;
import com.orientechnologies.orient.distributed.context.coordination.result.OAcceptResult;
import com.orientechnologies.orient.distributed.context.topology.ODiscoverAction;
import java.util.List;
import java.util.Optional;
import java.util.Set;

public interface OCoordinatedDistributedOps {

  // Methods for coordinations of structural operations not of nodes

  Optional<OOperationStart> start(OCompleteAction action);

  Optional<OAcceptResult> receive(ODistributedMessage message);

  void nodeSuccess(ONodeId node, OTransactionIdPromise promise);

  void nodeFailure(ONodeId node, OTransactionIdPromise promise, OAcceptResult acceptResult);

  Optional<ODistributedMessage> consensusFailure(OTransactionIdPromise promise);

  Optional<ODistributedMessage> consensusSuccess(OTransactionIdPromise promise);

  void completeExecution(OTransactionIdPromise promise);

  // Methods for coordinations of operations to add and remove nodes

  ODiscoverAction discoverNode(ONodeId node);

  ODiscoverAction nodeJoinStart(ONodeId node, ONodeStateNetwork state);

  Optional<OAcceptResult> promiseRegister(ONodeId node, long version);

  void registerNode(ONodeId node, long version);

  void unregisterNode(ONodeId node, long version);

  // Methods for coordinations of  operations to add establish the first network of nodes

  Optional<OTransactionIdPromise> startEstablish(Set<ONodeId> nodes, OCompleteAction action);

  Optional<OAcceptResult> validateEnstablish(OGroupId networkId, Set<ONodeId> candidates);

  Set<ONodeId> enstablish(OGroupId networkId, Set<ONodeId> candidates);

  Set<ONodeId> getMembers();

  ONodeStateNetwork getNetworkState();

  void load(Optional<ONodeStateStore> nodeStateStore, Optional<OTransactionSequenceStatus> status);

  void cancelRegisterPromise();

  void cancelEnstablish();

  OTransactionSequenceStatus getSequenceStatus();

  void loadSequence(OTransactionSequenceStatus status);

  // Methods for manage databases and node in the databases
  Optional<OAcceptResult> promiseDeclare(
      OTransactionIdPromise promise,
      ODatabaseId databaseId,
      String database,
      Set<ONodeId> partecipants,
      int minimumQuorum);

  void declareDatabase(
      OTransactionIdPromise promise,
      ODatabaseId dbId,
      String database,
      Set<ONodeId> partecipants,
      int minimumQuorum);

  void cancelDatabase(OTransactionIdPromise promise, ODatabaseId dbId, String database);

  Optional<OAcceptResult> promiseAddDatabaseMember(
      ODatabaseId dbId, List<OAddNodeInfo> nodes, long version);

  void addDatabaseMember(ODatabaseId dbId, List<OAddNodeInfo> nodes, long version);

  public void cancelAddDatabaseMember(ODatabaseId dbId, List<OAddNodeInfo> nodes);

  ODatabasesTopologyState getDatabaseTopology();
}
