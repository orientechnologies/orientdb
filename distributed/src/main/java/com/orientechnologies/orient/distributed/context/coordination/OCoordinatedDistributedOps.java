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
import com.orientechnologies.orient.distributed.context.coordination.dbs.ONodeRole;
import com.orientechnologies.orient.distributed.context.coordination.dbs.ONotificationAction;
import com.orientechnologies.orient.distributed.context.coordination.message.ODistributedMessage;
import com.orientechnologies.orient.distributed.context.coordination.message.operation.OAddNodeInfo;
import com.orientechnologies.orient.distributed.context.coordination.message.state.ONodeStateNetwork;
import com.orientechnologies.orient.distributed.context.coordination.result.OAcceptResult;
import com.orientechnologies.orient.distributed.context.coordination.sync.OSyncId;
import com.orientechnologies.orient.distributed.context.coordination.sync.OSyncInfo;
import com.orientechnologies.orient.distributed.context.coordination.sync.OSyncMode;
import com.orientechnologies.orient.distributed.context.coordination.sync.OSyncState;
import com.orientechnologies.orient.distributed.context.coordination.topology.ODiscoverAction;
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

  OVersion nextTopologyVersion();

  OVersion nextDatabaseVersion(ODatabaseId Id);

  // Methods for coordinations of operations to add and remove nodes

  ODiscoverAction discoverNode(ONodeId node);

  ODiscoverAction nodeJoinStart(ONodeId node, ONodeStateNetwork state, boolean merge);

  Optional<OAcceptResult> validateRegisterNode(
      ONodeId node, OVersion version, OTransactionIdPromise promise);

  void registerNode(ONodeId node, OVersion version, OTransactionIdPromise promise);

  Optional<OAcceptResult> validateUnregisterNode(
      ONodeId node, OVersion version, OTransactionIdPromise promise);

  ODisconnectAction unregisterNode(ONodeId node, OVersion version, OTransactionIdPromise promise);

  void cancelUnregisterNode(OTransactionIdPromise promise);

  void cancelRegisterNode(OTransactionIdPromise promise);

  // Methods to merge networks when possible
  Optional<OAcceptResult> validateMergeToNetwork(
      OGroupId group,
      ONodeStateNetwork state,
      ONodeStateNetwork original,
      OTransactionIdPromise coordinator);

  void mergeToNetwork(OTransactionIdPromise promise);

  void nodeMergeResult(
      ONodeId node, OTransactionIdPromise promise, Optional<OAcceptResult> accepted);

  void cancelMergeToNetwork(OTransactionIdPromise promise);

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
      ODatabaseId dbId, List<OAddNodeInfo> nodes, OVersion version, OTransactionIdPromise promise);

  void addDatabaseMember(
      ODatabaseId dbId, List<OAddNodeInfo> nodes, OVersion version, OTransactionIdPromise promise);

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
      OVersion version,
      OTransactionIdPromise promise);

  void setState(
      ODatabaseId db,
      ONodeId node,
      ODatabaseState state,
      OVersion version,
      OTransactionIdPromise promise);

  void cancelSetState(
      ODatabaseId dbId, ONodeId nodeId, OVersion version, OTransactionIdPromise promise);

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

  ONetworkTopology getNetworkTopology();

  ONodeStateNetwork getNetworkState();

  ONodeId getCurrent();

  OGroupId getGroupId();

  boolean isApplied(OTransactionId txId);

  // Load from persistent state
  void load(ONodeStateStore storeState);

  // Wait for events
  boolean executeOnOneOnline(ODatabaseId dbId, ONotificationAction execute);

  boolean waitOnlineQuorum(ODatabaseId dbId, Optional<Long> timeout) throws InterruptedException;

  public boolean waitSelfOnline(String dbName, Optional<Long> timeout) throws InterruptedException;

  public boolean waitSelfOnline(ODatabaseId dbId, Optional<Long> timeout)
      throws InterruptedException;

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

  Optional<OAcceptResult> validateMergeNode(
      ONodeId node,
      ONodeStateNetwork state,
      ONodeStateNetwork original,
      OTransactionIdPromise promise);

  void mergeNode(
      ONodeId node,
      ONodeStateNetwork state,
      ONodeStateNetwork original,
      OTransactionIdPromise promise);

  void cancelMergeNode(
      ONodeId node,
      ONodeStateNetwork state,
      ONodeStateNetwork original,
      OTransactionIdPromise promise);

  ONodeStateNetwork createMergedState(ONodeStateNetwork state);

  Optional<OAcceptResult> validateSetDatabaseNodeRole(
      ODatabaseId db,
      ONodeId node,
      ONodeRole role,
      OVersion version,
      OTransactionIdPromise promise);

  void setDatabaseNodeRole(
      ODatabaseId db,
      ONodeId node,
      ONodeRole role,
      OVersion version,
      OTransactionIdPromise promise);

  void cancelSetDatabaseNodeRole(
      ODatabaseId db,
      ONodeId node,
      ONodeRole role,
      OVersion version,
      OTransactionIdPromise promise);

  void dbRemovedFromDiskWhenOffline(ODatabaseId db);

  boolean waitForEnstablish(Optional<Long> timeout) throws InterruptedException;

  Optional<OAcceptResult> validateSetDatabaseQuorum(
      ODatabaseId db, int quorum, OVersion version, OTransactionIdPromise promise);

  void setDatabaseQuorum(
      ODatabaseId db, int quorum, OVersion version, OTransactionIdPromise promise);

  void cancelSetDatabaseQuorum(
      ODatabaseId db, int quorum, OVersion version, OTransactionIdPromise promise);
}
