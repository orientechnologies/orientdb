package com.orientechnologies.orient.distributed.context.coordination;

import com.orientechnologies.orient.core.transaction.ODatabaseId;
import com.orientechnologies.orient.core.transaction.OGroupId;
import com.orientechnologies.orient.core.transaction.ONodeId;
import com.orientechnologies.orient.core.transaction.OTransactionId;
import com.orientechnologies.orient.core.transaction.OTransactionIdPromise;
import com.orientechnologies.orient.core.tx.OTransactionSequenceStatus;
import com.orientechnologies.orient.distributed.context.ONodeStateStore;
import com.orientechnologies.orient.distributed.context.coordination.action.OCompleteAction;
import com.orientechnologies.orient.distributed.context.coordination.dbs.ODatabaseState;
import com.orientechnologies.orient.distributed.context.coordination.dbs.ODatabasesTopology;
import com.orientechnologies.orient.distributed.context.coordination.dbs.OStateAction;
import com.orientechnologies.orient.distributed.context.coordination.message.ODistributedMessage;
import com.orientechnologies.orient.distributed.context.coordination.message.operation.OAddNodeInfo;
import com.orientechnologies.orient.distributed.context.coordination.message.state.ONodeStateNetwork;
import com.orientechnologies.orient.distributed.context.coordination.result.OAcceptResult;
import com.orientechnologies.orient.distributed.context.coordination.sync.OSyncId;
import com.orientechnologies.orient.distributed.context.coordination.sync.OSyncInfo;
import com.orientechnologies.orient.distributed.context.coordination.sync.OSyncState;
import com.orientechnologies.orient.distributed.context.coordination.topology.ODiscoverAction;
import com.orientechnologies.orient.distributed.db.OSyncMode;
import java.util.List;
import java.util.Optional;
import java.util.Set;

public interface OCoordinatedDistributedOps {

  // Methods for coordinations of structural operations not of nodes

  Optional<OOperationStart> start(OCompleteAction action);

  Optional<OOperationStart> restart(OTransactionIdPromise current, OCompleteAction action);

  Optional<OAcceptResult> receive(ODistributedMessage message);

  void nodeSuccess(ONodeId node, OTransactionIdPromise promise);

  void nodeFailure(ONodeId node, OTransactionIdPromise promise, OAcceptResult acceptResult);

  Optional<ODistributedMessage> consensusFailure(OTransactionIdPromise promise);

  Optional<ODistributedMessage> consensusSuccess(OTransactionIdPromise promise);

  void completeExecution(OTransactionIdPromise promise);

  long nextTopologyVersion();

  long nextDatabaseVersion(ODatabaseId Id);

  // Methods for coordinations of operations to add and remove nodes

  ODiscoverAction discoverNode(ONodeId node);

  ODiscoverAction nodeJoinStart(ONodeId node, ONodeStateNetwork state, boolean merge);

  Optional<OAcceptResult> validateRegisterNode(
      ONodeId node, long version, OTransactionIdPromise promise);

  void registerNode(ONodeId node, long version, OTransactionIdPromise promise);

  Optional<OAcceptResult> validateUnregisterNode(
      ONodeId node, OVersion version, OTransactionIdPromise promise);

  ODisconnectAction unregisterNode(ONodeId node, OVersion version, OTransactionIdPromise promise);

  void cancelUnregisterNode(OTransactionIdPromise promise);

  void cancelRegisterNode(OTransactionIdPromise promise);

  // Methods to merge networks when possible
  Optional<OAcceptResult> validateMerge(OGroupId group, OTransactionIdPromise coordinator);

  void confirmMerge(ONodeId node, OTransactionIdPromise promise, Optional<OAcceptResult> accepted);

  void cancelMerge(OTransactionIdPromise promise);

  // Methods for coordinations of  operations to add establish the first network of nodes

  Optional<OTransactionIdPromise> startEstablish(Set<ONodeId> nodes, OCompleteAction action);

  Optional<OAcceptResult> validateEstablish(
      OGroupId networkId, Set<ONodeId> candidates, OTransactionIdPromise promise);

  Set<ONodeId> establish(
      OGroupId networkId, Set<ONodeId> candidates, OTransactionIdPromise promise);

  void cancelEstablish(OTransactionIdPromise promise);

  // Methods for manage databases and node in the databases

  Optional<OAcceptResult> validateDeclareDatabase(
      OTransactionIdPromise promise,
      ODatabaseId databaseId,
      String database,
      Set<OAddNodeInfo> partecipants,
      int minimumQuorum);

  void declareDatabase(
      OTransactionIdPromise promise,
      ODatabaseId dbId,
      String database,
      Set<OAddNodeInfo> partecipants,
      int minimumQuorum);

  void cancelDeclareDatabase(OTransactionIdPromise promise, ODatabaseId dbId, String database);

  // Methods for manage the add of nodes to database

  Optional<OAcceptResult> validateAddDatabaseMember(
      ODatabaseId dbId, List<OAddNodeInfo> nodes, long version, OTransactionIdPromise promise);

  void addDatabaseMember(
      ODatabaseId dbId, List<OAddNodeInfo> nodes, long version, OTransactionIdPromise promise);

  void cancelAddDatabaseMember(
      ODatabaseId dbId, List<OAddNodeInfo> nodes, OTransactionIdPromise promise);

  // Methods for manage the remove of nodes to database

  Optional<OAcceptResult> validateRemoveDatabaseMembers(
      ODatabaseId dbId, List<ONodeId> nodes, OVersion version, OTransactionIdPromise promise);

  void removeDatabaseMembers(
      ODatabaseId dbId, List<ONodeId> nodes, OVersion version, OTransactionIdPromise promise);

  void cancelRemoveDatabaseMembers(
      ODatabaseId dbId, List<ONodeId> nodes, OTransactionIdPromise promise);

  // Methods to manage the change state of a database on a specific node

  Optional<OAcceptResult> validateSetState(
      ODatabaseId dbId,
      ONodeId nodeId,
      ODatabaseState state,
      long version,
      OTransactionIdPromise promise);

  void setState(
      ODatabaseId db,
      ONodeId node,
      ODatabaseState state,
      long version,
      OTransactionIdPromise promise);

  void cancelSetState(
      ODatabaseId dbId, ONodeId nodeId, long version, OTransactionIdPromise promise);

  // Methods for manage the sync flow

  Optional<OSyncInfo> newSync(ODatabaseId dbId);

  boolean acceptSync(ONodeId sender, ONodeId receiver, ODatabaseId dbId, OSyncId syncId);

  Optional<OSyncState> canSync(
      ONodeId sender,
      ONodeId receiver,
      ODatabaseId dbId,
      OSyncId syncId,
      boolean canSync,
      OSyncMode mode,
      Optional<OTransactionSequenceStatus> sequenceStatus);

  OSyncState startSend(
      ONodeId to,
      ONodeId from,
      ODatabaseId dbId,
      OSyncId syncId,
      OSyncMode mode,
      Optional<OTransactionSequenceStatus> sequenceStatus);

  OSyncState getSyncState(OSyncId syncId);

  void completeSync(OSyncId syncId);

  // State Reading
  ODatabasesTopology getDatabaseTopology();

  Set<ONodeId> getNetworkMembers();

  ONodeStateNetwork getNetworkState();

  ONodeId getCurrent();

  OGroupId getGroupId();

  boolean isApplied(OTransactionId txId);

  // Load from persistent state
  void load(ONodeStateStore storeState);

  // Wait for events
  boolean executeOnOneOnline(ODatabaseId dbId, OStateAction execute);

  boolean waitOnlineQuorum(ODatabaseId dbId, Optional<Long> timeout) throws InterruptedException;

  // Network events

  ODisconnectAction nodeDisconnected(ONodeId node);

  void cancelPromise(OTransactionIdPromise promise);

  Optional<OAcceptResult> receiveRetry(OTransactionIdPromise promise);

  OTransactionSequenceStatus getTransactionSequenceState();

  void receivePing(ONodeId nodeId, OTransactionSequenceStatus status);

  Set<ONodeId> checkOffline(long time);

  int getDatabaseQuorum(ODatabaseId databaseId);

  Optional<OAcceptResult> validateDropDatabase(
      ODatabaseId dbId, OVersion version, OTransactionIdPromise promise);

  void dropDatabase(ODatabaseId dbId, OVersion version, OTransactionIdPromise promise);

  void cancelDropDatabase(ODatabaseId dbId, OVersion version, OTransactionIdPromise promise);

  int getTopologyQuorum();
}
