package com.orientechnologies.orient.distriubted.context;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import com.orientechnologies.orient.core.transaction.ODatabaseId;
import com.orientechnologies.orient.core.transaction.ONodeId;
import com.orientechnologies.orient.core.transaction.OTransactionId;
import com.orientechnologies.orient.core.transaction.OTransactionIdPromise;
import com.orientechnologies.orient.distributed.context.coordination.dbs.ODatabaseState;
import com.orientechnologies.orient.distributed.context.coordination.dbs.ODatabaseStateChangeListener;
import com.orientechnologies.orient.distributed.context.coordination.dbs.ODatabasesTopologyState;
import com.orientechnologies.orient.distributed.context.coordination.dbs.ONodeRole;
import com.orientechnologies.orient.distributed.context.coordination.message.operation.OAddNodeInfo;
import com.orientechnologies.orient.distributed.context.coordination.result.OAcceptResult;
import com.orientechnologies.orient.distributed.context.coordination.result.OAlreadyPromised;
import com.orientechnologies.orient.distributed.context.coordination.result.ONodeAlreadyPresent;
import com.orientechnologies.orient.distributed.context.coordination.result.OOutdatedVersion;
import com.orientechnologies.orient.distributed.context.coordination.sync.OSyncInfo;
import com.orientechnologies.orient.distributed.context.coordination.sync.OSyncState;
import com.orientechnologies.orient.distributed.db.OSyncMode;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import org.junit.Test;

public class ODatabasesTopologyStateTest implements ODatabaseStateChangeListener {

  private ONodeId newNodeId() {
    return new ONodeId(UUID.randomUUID().toString());
  }

  private ODatabaseId newDbId() {
    return new ODatabaseId(UUID.randomUUID().toString());
  }

  private OTransactionIdPromise newPromiseId() {
    return new OTransactionIdPromise(newNodeId(), new OTransactionId(10, 20));
  }

  @Override
  public void onStateChange(ODatabaseId dbId, ONodeId nodeId, ODatabaseState state) {}

  @Test
  public void testFirstDeclare() {

    ODatabasesTopologyState state = new ODatabasesTopologyState(this, newNodeId());
    var promiseId = newPromiseId();
    Set<OAddNodeInfo> partecipants = partecipants(promiseId.getCoordinator());
    var dbId = newDbId();
    String name = "dbName";
    int quorum = 2;
    var res = state.validateDeclare(promiseId, dbId, name, partecipants, quorum);
    assertTrue(res.isEmpty());

    state.declareDatabase(promiseId, dbId, name, partecipants, quorum);
    assertTrue(state.listDatabaseIds().contains(dbId));
  }

  @Test
  public void testDeclareAndPassState() {

    ODatabasesTopologyState state = new ODatabasesTopologyState(this, newNodeId());
    var promiseId = newPromiseId();
    Set<OAddNodeInfo> partecipants = partecipants(promiseId.getCoordinator());
    var dbId = newDbId();
    String name = "dbName";
    int quorum = 2;
    var res = state.validateDeclare(promiseId, dbId, name, partecipants, quorum);
    assertTrue(res.isEmpty());

    state.declareDatabase(promiseId, dbId, name, partecipants, quorum);
    assertTrue(state.listDatabaseIds().contains(dbId));

    ODatabasesTopologyState state1 = new ODatabasesTopologyState(this, newNodeId());
    state1.receiverNetworkState(state.getNetworkState());
    assertEquals(dbId, state1.getDatabaseId("dbName").get());
    assertEquals("dbName", state1.getDatabaseName(dbId));
  }

  @Test
  public void testFailDoublePromise() {

    ODatabasesTopologyState state = new ODatabasesTopologyState(this, newNodeId());

    var promiseId = newPromiseId();
    Set<OAddNodeInfo> partecipants = partecipants(promiseId.getCoordinator());
    var dbId = newDbId();
    String name = "dbName";
    int quorum = 2;
    var res = state.validateDeclare(promiseId, dbId, name, partecipants, quorum);
    assertTrue(res.isEmpty());

    var promiseId1 = newPromiseId();

    res = state.validateDeclare(promiseId1, dbId, name, partecipants, quorum);
    assertTrue(res.isPresent());
    assertTrue(res.get() instanceof OAlreadyPromised);

    var promiseId2 = newPromiseId();
    var dbId1 = newDbId();
    res = state.validateDeclare(promiseId2, dbId1, name, partecipants, quorum);
    assertTrue(res.isPresent());
    assertTrue(res.get() instanceof OAlreadyPromised);
  }

  @Test
  public void testOkAfterCancelPromise() {

    ODatabasesTopologyState state = new ODatabasesTopologyState(this, newNodeId());

    var promiseId = newPromiseId();
    Set<OAddNodeInfo> partecipants = partecipants(promiseId.getCoordinator());
    var dbId = newDbId();
    String name = "dbName";
    int quorum = 2;
    var res = state.validateDeclare(promiseId, dbId, name, partecipants, quorum);
    assertTrue(res.isEmpty());

    var promiseId1 = newPromiseId();

    res = state.validateDeclare(promiseId1, dbId, name, partecipants, quorum);
    assertTrue(res.isPresent());
    assertTrue(res.get() instanceof OAlreadyPromised);

    state.cancelPomise(promiseId, dbId, name);

    res = state.validateDeclare(promiseId1, dbId, name, partecipants, quorum);
    assertTrue(res.isEmpty());
  }

  @Test
  public void testSetStateBase() {

    ODatabasesTopologyState state = new ODatabasesTopologyState(this, newNodeId());

    var promiseId = newPromiseId();
    ONodeId nodeId = promiseId.getCoordinator();
    Set<OAddNodeInfo> partecipants = partecipants(nodeId);
    var dbId = newDbId();
    String name = "dbName";
    int quorum = 2;
    var res = state.validateDeclare(promiseId, dbId, name, partecipants, quorum);
    assertTrue(res.isEmpty());

    state.declareDatabase(promiseId, dbId, name, partecipants, quorum);

    ODatabaseState ns = state.getState(dbId, nodeId);
    assertEquals(ns, ODatabaseState.Offline);

    state.validateSetState(dbId, nodeId, ODatabaseState.Online, 1L, promiseId);

    state.setState(dbId, nodeId, ODatabaseState.Online, 1L, promiseId);

    ns = state.getState(dbId, nodeId);
    assertEquals(ns, ODatabaseState.Online);
  }

  @Test
  public void testSetStateDoublePromise() {

    ODatabasesTopologyState state = new ODatabasesTopologyState(this, newNodeId());

    var promiseId = newPromiseId();
    ONodeId nodeId = promiseId.getCoordinator();
    Set<OAddNodeInfo> partecipants = partecipants(nodeId);
    var dbId = newDbId();
    String name = "dbName";
    int quorum = 2;
    var res = state.validateDeclare(promiseId, dbId, name, partecipants, quorum);
    assertTrue(res.isEmpty());

    state.declareDatabase(promiseId, dbId, name, partecipants, quorum);

    ODatabaseState ns = state.getState(dbId, nodeId);
    assertEquals(ns, ODatabaseState.Offline);

    ONodeId newNode = newNodeId();
    var addInfos = List.of(new OAddNodeInfo(newNode, ONodeRole.Main));
    var nextVersion = state.getDatabaseVersion(dbId) + 1;
    state.validateAddMember(dbId, addInfos, nextVersion, promiseId);
    state.addDatabaseMember(dbId, addInfos, nextVersion, promiseId);
    var nextVersion1 = state.getDatabaseVersion(dbId) + 1;
    Optional<OAcceptResult> prom =
        state.validateSetState(dbId, nodeId, ODatabaseState.Online, nextVersion1, promiseId);
    assertTrue(prom.isEmpty());

    Optional<OAcceptResult> prom1 =
        state.validateSetState(dbId, newNode, ODatabaseState.Online, nextVersion1, promiseId);
    assertTrue(prom1.isPresent());
    assertTrue(prom1.get() instanceof OAlreadyPromised);

    state.setState(dbId, nodeId, ODatabaseState.Online, 1L, promiseId);

    ns = state.getState(dbId, nodeId);
    assertEquals(ns, ODatabaseState.Online);
  }

  @Test
  public void testSetStateWrongVersion() {

    ODatabasesTopologyState state = new ODatabasesTopologyState(this, newNodeId());

    var promiseId = newPromiseId();
    ONodeId nodeId = promiseId.getCoordinator();
    Set<OAddNodeInfo> partecipants = partecipants(nodeId);
    var dbId = newDbId();
    String name = "dbName";
    int quorum = 2;
    var res = state.validateDeclare(promiseId, dbId, name, partecipants, quorum);
    assertTrue(res.isEmpty());

    state.declareDatabase(promiseId, dbId, name, partecipants, quorum);

    ODatabaseState ns = state.getState(dbId, nodeId);
    assertEquals(ns, ODatabaseState.Offline);

    Optional<OAcceptResult> prom =
        state.validateSetState(dbId, nodeId, ODatabaseState.Online, 1L, promiseId);
    assertTrue(prom.isEmpty());

    state.setState(dbId, nodeId, ODatabaseState.Online, 1L, promiseId);

    ns = state.getState(dbId, nodeId);
    assertEquals(ns, ODatabaseState.Online);

    ONodeId newNode = newNodeId();
    var addInfos = List.of(new OAddNodeInfo(newNode, ONodeRole.Main));
    var nextVersion = state.getDatabaseVersion(dbId) + 1;
    state.validateAddMember(dbId, addInfos, nextVersion, promiseId);
    state.addDatabaseMember(dbId, addInfos, nextVersion, promiseId);

    Optional<OAcceptResult> prom1 =
        state.validateSetState(dbId, newNode, ODatabaseState.Offline, 1L, promiseId);
    assertTrue(prom1.isPresent());
    assertTrue(prom1.get() instanceof OOutdatedVersion);
  }

  @Test
  public void testSetStateCancelPromise() {

    ODatabasesTopologyState state = new ODatabasesTopologyState(this, newNodeId());

    var promiseId = newPromiseId();
    ONodeId nodeId = promiseId.getCoordinator();
    Set<OAddNodeInfo> partecipants = partecipants(nodeId);
    var dbId = newDbId();
    String name = "dbName";
    int quorum = 2;
    var res = state.validateDeclare(promiseId, dbId, name, partecipants, quorum);
    assertTrue(res.isEmpty());

    state.declareDatabase(promiseId, dbId, name, partecipants, quorum);

    ODatabaseState ns = state.getState(dbId, nodeId);
    assertEquals(ns, ODatabaseState.Offline);

    Optional<OAcceptResult> prom =
        state.validateSetState(dbId, nodeId, ODatabaseState.Online, 1L, promiseId);
    assertTrue(prom.isEmpty());
    state.cancelSetState(dbId, nodeId, 1L, promiseId);

    Optional<OAcceptResult> prom1 =
        state.validateSetState(dbId, nodeId, ODatabaseState.Online, 1L, promiseId);
    assertTrue(prom1.isEmpty());

    state.setState(dbId, nodeId, ODatabaseState.Online, 1L, promiseId);

    ns = state.getState(dbId, nodeId);
    assertEquals(ns, ODatabaseState.Online);
  }

  private Set<OAddNodeInfo> partecipants(ONodeId... nodes) {
    Set<OAddNodeInfo> set = new HashSet<>(nodes.length);
    for (ONodeId node : nodes) {
      set.add(new OAddNodeInfo(node, ONodeRole.Main));
    }
    return set;
  }

  @Test
  public void testRequestSync() {

    ODatabasesTopologyState state = new ODatabasesTopologyState(this, newNodeId());
    ODatabasesTopologyState state1 = new ODatabasesTopologyState(this, newNodeId());

    var promiseId = newPromiseId();
    ONodeId nodeId = promiseId.getCoordinator();
    ONodeId node1 = newNodeId();
    ONodeId node2 = newNodeId();

    Set<OAddNodeInfo> partecipants = partecipants(nodeId, node1, node2);
    var dbId = newDbId();
    String name = "dbName";
    int quorum = 2;
    var res = state.validateDeclare(promiseId, dbId, name, partecipants, quorum);
    assertTrue(res.isEmpty());
    var res1 = state1.validateDeclare(promiseId, dbId, name, partecipants, quorum);
    assertTrue(res1.isEmpty());

    state.declareDatabase(promiseId, dbId, name, partecipants, quorum);
    state1.declareDatabase(promiseId, dbId, name, partecipants, quorum);

    ODatabaseState ns = state.getState(dbId, nodeId);
    assertEquals(ns, ODatabaseState.Offline);

    Optional<OAcceptResult> prom =
        state.validateSetState(dbId, nodeId, ODatabaseState.Online, 1L, promiseId);

    assertTrue(prom.isEmpty());
    Optional<OAcceptResult> prom1 =
        state1.validateSetState(dbId, nodeId, ODatabaseState.Online, 1L, promiseId);
    assertTrue(prom1.isEmpty());

    state.setState(dbId, nodeId, ODatabaseState.Online, 1L, promiseId);
    state1.setState(dbId, nodeId, ODatabaseState.Online, 1L, promiseId);

    OSyncInfo syncInfo = state1.newSync(dbId).get();
    assertTrue(syncInfo.targets().contains(nodeId));
    boolean canSync = state.acceptSync(nodeId, node1, dbId, syncInfo.syncId());
    assertTrue(canSync);
    Optional<OSyncState> receiverStateOp =
        state1.canSync(
            nodeId,
            node1,
            dbId,
            syncInfo.syncId(),
            canSync,
            OSyncMode.StandardBackup,
            Optional.empty());
    assertTrue(receiverStateOp.isPresent());
    OSyncState receiverState = receiverStateOp.get();

    OSyncState senderState =
        state.startSend(
            node1, nodeId, dbId, syncInfo.syncId(), OSyncMode.StandardBackup, Optional.empty());

    assertEquals(receiverState.getSender(), senderState.getSender());
    assertEquals(receiverState.getReceiver(), senderState.getReceiver());
    assertEquals(receiverState.getSyncId(), senderState.getSyncId());
    assertEquals(receiverState.getDbId(), senderState.getDbId());
    assertEquals(receiverState.getMode(), senderState.getMode());

    OSyncState ss = state.getSyncState(senderState.getSyncId());
    assertSame(senderState, ss);

    OSyncState rs = state1.getSyncState(receiverState.getSyncId());
    assertSame(receiverState, rs);
  }

  @Test
  public void testRequestSyncFailAlreadySendings() {

    ODatabasesTopologyState state = new ODatabasesTopologyState(this, newNodeId());
    ODatabasesTopologyState state1 = new ODatabasesTopologyState(this, newNodeId());
    ODatabasesTopologyState state2 = new ODatabasesTopologyState(this, newNodeId());

    var promiseId = newPromiseId();
    ONodeId nodeId = promiseId.getCoordinator();
    ONodeId node1 = newNodeId();
    ONodeId node2 = newNodeId();

    Set<OAddNodeInfo> partecipants = partecipants(nodeId, node1, node2);
    var dbId = newDbId();
    String name = "dbName";
    int quorum = 2;
    var res = state.validateDeclare(promiseId, dbId, name, partecipants, quorum);
    assertTrue(res.isEmpty());
    var res1 = state1.validateDeclare(promiseId, dbId, name, partecipants, quorum);
    assertTrue(res1.isEmpty());
    var res2 = state2.validateDeclare(promiseId, dbId, name, partecipants, quorum);
    assertTrue(res2.isEmpty());

    state.declareDatabase(promiseId, dbId, name, partecipants, quorum);
    state1.declareDatabase(promiseId, dbId, name, partecipants, quorum);
    state2.declareDatabase(promiseId, dbId, name, partecipants, quorum);

    ODatabaseState ns = state.getState(dbId, nodeId);
    assertEquals(ns, ODatabaseState.Offline);

    Optional<OAcceptResult> prom =
        state.validateSetState(dbId, nodeId, ODatabaseState.Online, 1L, promiseId);

    assertTrue(prom.isEmpty());
    Optional<OAcceptResult> prom1 =
        state1.validateSetState(dbId, nodeId, ODatabaseState.Online, 1L, promiseId);
    assertTrue(prom1.isEmpty());
    Optional<OAcceptResult> prom2 =
        state2.validateSetState(dbId, nodeId, ODatabaseState.Online, 1L, promiseId);
    assertTrue(prom2.isEmpty());

    state.setState(dbId, nodeId, ODatabaseState.Online, 1L, promiseId);
    state1.setState(dbId, nodeId, ODatabaseState.Online, 1L, promiseId);
    state2.setState(dbId, nodeId, ODatabaseState.Online, 1L, promiseId);

    OSyncInfo syncInfo = state1.newSync(dbId).get();
    assertTrue(syncInfo.targets().contains(nodeId));
    boolean canSync = state.acceptSync(nodeId, node1, dbId, syncInfo.syncId());
    assertTrue(canSync);
    Optional<OSyncState> receiverStateOp =
        state1.canSync(
            nodeId,
            node1,
            dbId,
            syncInfo.syncId(),
            canSync,
            OSyncMode.StandardBackup,
            Optional.empty());
    assertTrue(receiverStateOp.isPresent());
    OSyncState receiverState = receiverStateOp.get();

    OSyncState senderState =
        state.startSend(
            node1, nodeId, dbId, syncInfo.syncId(), OSyncMode.StandardBackup, Optional.empty());

    assertEquals(receiverState.getSender(), senderState.getSender());
    assertEquals(receiverState.getReceiver(), senderState.getReceiver());
    assertEquals(receiverState.getSyncId(), senderState.getSyncId());
    assertEquals(receiverState.getDbId(), senderState.getDbId());
    assertEquals(receiverState.getMode(), senderState.getMode());

    OSyncState ss = state.getSyncState(senderState.getSyncId());
    assertSame(senderState, ss);

    OSyncState rs = state1.getSyncState(receiverState.getSyncId());
    assertSame(receiverState, rs);

    OSyncInfo syncInfo2 = state2.newSync(dbId).get();
    assertTrue(syncInfo2.targets().contains(nodeId));
    boolean canSync2 = state.acceptSync(nodeId, node2, dbId, syncInfo.syncId());
    assertFalse(canSync2);
  }

  @Test
  public void testSetAddMember() {

    ODatabasesTopologyState state = new ODatabasesTopologyState(this, newNodeId());

    var promiseId = newPromiseId();
    ONodeId nodeId = promiseId.getCoordinator();
    ONodeId nodeId1 = newNodeId();
    Set<OAddNodeInfo> partecipants = partecipants(nodeId);
    var dbId = newDbId();
    String name = "dbName";
    int quorum = 2;
    var res = state.validateDeclare(promiseId, dbId, name, partecipants, quorum);
    assertTrue(res.isEmpty());

    state.declareDatabase(promiseId, dbId, name, partecipants, quorum);

    long version = state.getDatabaseVersion(dbId) + 1;
    var nodes = List.of(new OAddNodeInfo(nodeId1, ONodeRole.Main));
    Optional<OAcceptResult> accept = state.validateAddMember(dbId, nodes, version, promiseId);
    assertTrue(accept.isEmpty());
    state.addDatabaseMember(dbId, nodes, version, promiseId);
    assertEquals(state.getState(dbId, nodeId1), ODatabaseState.Offline);

    Optional<OAcceptResult> acceptAfter = state.validateAddMember(dbId, nodes, version, promiseId);
    assertTrue(acceptAfter.get() instanceof ONodeAlreadyPresent);
  }

  @Test
  public void testSetAddMemberCancel() {

    ODatabasesTopologyState state = new ODatabasesTopologyState(this, newNodeId());

    var promiseId = newPromiseId();
    ONodeId nodeId = promiseId.getCoordinator();
    ONodeId nodeId1 = newNodeId();
    Set<OAddNodeInfo> partecipants = partecipants(nodeId);
    var dbId = newDbId();
    String name = "dbName";
    int quorum = 2;
    var res = state.validateDeclare(promiseId, dbId, name, partecipants, quorum);
    assertTrue(res.isEmpty());

    state.declareDatabase(promiseId, dbId, name, partecipants, quorum);

    long version = state.getDatabaseVersion(dbId) + 1;
    var nodes = List.of(new OAddNodeInfo(nodeId1, ONodeRole.Main));
    Optional<OAcceptResult> accept = state.validateAddMember(dbId, nodes, version, promiseId);
    assertTrue(accept.isEmpty());
    state.cancelAddDatabaseMember(dbId, nodes, promiseId);
    assertEquals(state.getState(dbId, nodeId1), ODatabaseState.NotAvailable);

    Optional<OAcceptResult> acceptAfter = state.validateAddMember(dbId, nodes, version, promiseId);
    assertTrue(acceptAfter.isEmpty());
  }
}
