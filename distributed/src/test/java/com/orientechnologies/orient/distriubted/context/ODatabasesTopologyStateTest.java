package com.orientechnologies.orient.distriubted.context;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import com.orientechnologies.orient.core.transaction.ODatabaseId;
import com.orientechnologies.orient.core.transaction.ONodeId;
import com.orientechnologies.orient.core.transaction.OTransactionId;
import com.orientechnologies.orient.core.transaction.OTransactionIdPromise;
import com.orientechnologies.orient.distributed.context.ODatabaseState;
import com.orientechnologies.orient.distributed.context.ODatabaseStateChangeListener;
import com.orientechnologies.orient.distributed.context.ODatabasesTopologyState;
import com.orientechnologies.orient.distributed.context.OSyncInfo;
import com.orientechnologies.orient.distributed.context.OSyncState;
import com.orientechnologies.orient.distributed.context.coordination.result.OAcceptResult;
import com.orientechnologies.orient.distributed.context.coordination.result.OAlreadyPromised;
import com.orientechnologies.orient.distributed.context.coordination.result.OInvalidSequential;
import com.orientechnologies.orient.distributed.db.OSyncMode;
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

    ODatabasesTopologyState state = new ODatabasesTopologyState(this);
    var promiseId = newPromiseId();
    Set<ONodeId> partecipants = Set.of(promiseId.getCoordinator());
    var dbId = newDbId();
    String name = "dbName";
    int quorum = 2;
    var res = state.promiseDeclare(promiseId, dbId, name, partecipants, quorum);
    assertTrue(res.isEmpty());

    state.declareDatabase(promiseId, dbId, name, partecipants, quorum);
    assertTrue(state.listDatabaseIds().contains(dbId));
  }

  @Test
  public void testFailDoublePromise() {

    ODatabasesTopologyState state = new ODatabasesTopologyState(this);

    var promiseId = newPromiseId();
    Set<ONodeId> partecipants = Set.of(promiseId.getCoordinator());
    var dbId = newDbId();
    String name = "dbName";
    int quorum = 2;
    var res = state.promiseDeclare(promiseId, dbId, name, partecipants, quorum);
    assertTrue(res.isEmpty());

    var promiseId1 = newPromiseId();

    res = state.promiseDeclare(promiseId1, dbId, name, partecipants, quorum);
    assertTrue(res.isPresent());
    assertTrue(res.get() instanceof OAlreadyPromised);

    var promiseId2 = newPromiseId();
    var dbId1 = newDbId();
    res = state.promiseDeclare(promiseId2, dbId1, name, partecipants, quorum);
    assertTrue(res.isPresent());
    assertTrue(res.get() instanceof OAlreadyPromised);
  }

  @Test
  public void testOkAfterCancelPromise() {

    ODatabasesTopologyState state = new ODatabasesTopologyState(this);

    var promiseId = newPromiseId();
    Set<ONodeId> partecipants = Set.of(promiseId.getCoordinator());
    var dbId = newDbId();
    String name = "dbName";
    int quorum = 2;
    var res = state.promiseDeclare(promiseId, dbId, name, partecipants, quorum);
    assertTrue(res.isEmpty());

    var promiseId1 = newPromiseId();

    res = state.promiseDeclare(promiseId1, dbId, name, partecipants, quorum);
    assertTrue(res.isPresent());
    assertTrue(res.get() instanceof OAlreadyPromised);

    state.cancelPomise(promiseId, dbId, name);

    res = state.promiseDeclare(promiseId1, dbId, name, partecipants, quorum);
    assertTrue(res.isEmpty());
  }

  @Test
  public void testSetStateBase() {

    ODatabasesTopologyState state = new ODatabasesTopologyState(this);

    var promiseId = newPromiseId();
    ONodeId nodeId = promiseId.getCoordinator();
    Set<ONodeId> partecipants = Set.of(nodeId);
    var dbId = newDbId();
    String name = "dbName";
    int quorum = 2;
    var res = state.promiseDeclare(promiseId, dbId, name, partecipants, quorum);
    assertTrue(res.isEmpty());

    state.declareDatabase(promiseId, dbId, name, partecipants, quorum);

    ODatabaseState ns = state.getNodeState(dbId, nodeId);
    assertEquals(ns, ODatabaseState.Offline);

    state.promiseState(dbId, nodeId, ODatabaseState.Online, 1L);

    state.setState(dbId, nodeId, ODatabaseState.Online, 1L);

    ns = state.getNodeState(dbId, nodeId);
    assertEquals(ns, ODatabaseState.Online);
  }

  @Test
  public void testSetStateDoublePromise() {

    ODatabasesTopologyState state = new ODatabasesTopologyState(this);

    var promiseId = newPromiseId();
    ONodeId nodeId = promiseId.getCoordinator();
    Set<ONodeId> partecipants = Set.of(nodeId);
    var dbId = newDbId();
    String name = "dbName";
    int quorum = 2;
    var res = state.promiseDeclare(promiseId, dbId, name, partecipants, quorum);
    assertTrue(res.isEmpty());

    state.declareDatabase(promiseId, dbId, name, partecipants, quorum);

    ODatabaseState ns = state.getNodeState(dbId, nodeId);
    assertEquals(ns, ODatabaseState.Offline);

    Optional<OAcceptResult> prom = state.promiseState(dbId, nodeId, ODatabaseState.Online, 1L);
    assertTrue(prom.isEmpty());

    Optional<OAcceptResult> prom1 =
        state.promiseState(dbId, newNodeId(), ODatabaseState.Online, 1L);
    assertTrue(prom1.isPresent());
    assertTrue(prom1.get() instanceof OAlreadyPromised);

    state.setState(dbId, nodeId, ODatabaseState.Online, 1L);

    ns = state.getNodeState(dbId, nodeId);
    assertEquals(ns, ODatabaseState.Online);
  }

  @Test
  public void testSetStateWrongVersion() {

    ODatabasesTopologyState state = new ODatabasesTopologyState(this);

    var promiseId = newPromiseId();
    ONodeId nodeId = promiseId.getCoordinator();
    Set<ONodeId> partecipants = Set.of(nodeId);
    var dbId = newDbId();
    String name = "dbName";
    int quorum = 2;
    var res = state.promiseDeclare(promiseId, dbId, name, partecipants, quorum);
    assertTrue(res.isEmpty());

    state.declareDatabase(promiseId, dbId, name, partecipants, quorum);

    ODatabaseState ns = state.getNodeState(dbId, nodeId);
    assertEquals(ns, ODatabaseState.Offline);

    Optional<OAcceptResult> prom = state.promiseState(dbId, nodeId, ODatabaseState.Online, 1L);
    assertTrue(prom.isEmpty());

    state.setState(dbId, nodeId, ODatabaseState.Online, 1L);

    ns = state.getNodeState(dbId, nodeId);
    assertEquals(ns, ODatabaseState.Online);

    Optional<OAcceptResult> prom1 =
        state.promiseState(dbId, newNodeId(), ODatabaseState.Offline, 1L);
    assertTrue(prom1.isPresent());
    assertTrue(prom1.get() instanceof OInvalidSequential);
  }

  @Test
  public void testSetStateCancelPromise() {

    ODatabasesTopologyState state = new ODatabasesTopologyState(this);

    var promiseId = newPromiseId();
    ONodeId nodeId = promiseId.getCoordinator();
    Set<ONodeId> partecipants = Set.of(nodeId);
    var dbId = newDbId();
    String name = "dbName";
    int quorum = 2;
    var res = state.promiseDeclare(promiseId, dbId, name, partecipants, quorum);
    assertTrue(res.isEmpty());

    state.declareDatabase(promiseId, dbId, name, partecipants, quorum);

    ODatabaseState ns = state.getNodeState(dbId, nodeId);
    assertEquals(ns, ODatabaseState.Offline);

    Optional<OAcceptResult> prom = state.promiseState(dbId, nodeId, ODatabaseState.Online, 1L);
    assertTrue(prom.isEmpty());
    state.cancelPomiseSetState(dbId, nodeId, 1L);

    Optional<OAcceptResult> prom1 =
        state.promiseState(dbId, newNodeId(), ODatabaseState.Online, 1L);
    assertTrue(prom1.isEmpty());

    state.setState(dbId, nodeId, ODatabaseState.Online, 1L);

    ns = state.getNodeState(dbId, nodeId);
    assertEquals(ns, ODatabaseState.Online);
  }

  @Test
  public void testRequestSync() {

    ODatabasesTopologyState state = new ODatabasesTopologyState(this);
    ODatabasesTopologyState state1 = new ODatabasesTopologyState(this);

    var promiseId = newPromiseId();
    ONodeId nodeId = promiseId.getCoordinator();
    ONodeId node1 = newNodeId();
    ONodeId node2 = newNodeId();

    Set<ONodeId> partecipants = Set.of(nodeId, node1, node2);
    var dbId = newDbId();
    String name = "dbName";
    int quorum = 2;
    var res = state.promiseDeclare(promiseId, dbId, name, partecipants, quorum);
    assertTrue(res.isEmpty());
    var res1 = state1.promiseDeclare(promiseId, dbId, name, partecipants, quorum);
    assertTrue(res1.isEmpty());

    state.declareDatabase(promiseId, dbId, name, partecipants, quorum);
    state1.declareDatabase(promiseId, dbId, name, partecipants, quorum);

    ODatabaseState ns = state.getNodeState(dbId, nodeId);
    assertEquals(ns, ODatabaseState.Offline);

    Optional<OAcceptResult> prom = state.promiseState(dbId, nodeId, ODatabaseState.Online, 1L);

    assertTrue(prom.isEmpty());
    Optional<OAcceptResult> prom1 = state1.promiseState(dbId, nodeId, ODatabaseState.Online, 1L);
    assertTrue(prom1.isEmpty());

    state.setState(dbId, nodeId, ODatabaseState.Online, 1L);
    state1.setState(dbId, nodeId, ODatabaseState.Online, 1L);

    OSyncInfo syncInfo = state1.newSync(dbId).get();
    assertTrue(syncInfo.targets().contains(nodeId));
    boolean canSync = state.acceptSync(nodeId, node1, dbId, syncInfo.syncId());
    assertTrue(canSync);
    Optional<OSyncState> receiverStateOp =
        state1.canSync(nodeId, node1, dbId, syncInfo.syncId(), canSync, OSyncMode.StandardBackup);
    assertTrue(receiverStateOp.isPresent());
    OSyncState receiverState = receiverStateOp.get();

    OSyncState senderState =
        state.startSend(node1, nodeId, dbId, syncInfo.syncId(), OSyncMode.StandardBackup);

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

    ODatabasesTopologyState state = new ODatabasesTopologyState(this);
    ODatabasesTopologyState state1 = new ODatabasesTopologyState(this);
    ODatabasesTopologyState state2 = new ODatabasesTopologyState(this);

    var promiseId = newPromiseId();
    ONodeId nodeId = promiseId.getCoordinator();
    ONodeId node1 = newNodeId();
    ONodeId node2 = newNodeId();

    Set<ONodeId> partecipants = Set.of(nodeId, node1, node2);
    var dbId = newDbId();
    String name = "dbName";
    int quorum = 2;
    var res = state.promiseDeclare(promiseId, dbId, name, partecipants, quorum);
    assertTrue(res.isEmpty());
    var res1 = state1.promiseDeclare(promiseId, dbId, name, partecipants, quorum);
    assertTrue(res1.isEmpty());
    var res2 = state2.promiseDeclare(promiseId, dbId, name, partecipants, quorum);
    assertTrue(res2.isEmpty());

    state.declareDatabase(promiseId, dbId, name, partecipants, quorum);
    state1.declareDatabase(promiseId, dbId, name, partecipants, quorum);
    state2.declareDatabase(promiseId, dbId, name, partecipants, quorum);

    ODatabaseState ns = state.getNodeState(dbId, nodeId);
    assertEquals(ns, ODatabaseState.Offline);

    Optional<OAcceptResult> prom = state.promiseState(dbId, nodeId, ODatabaseState.Online, 1L);

    assertTrue(prom.isEmpty());
    Optional<OAcceptResult> prom1 = state1.promiseState(dbId, nodeId, ODatabaseState.Online, 1L);
    assertTrue(prom1.isEmpty());
    Optional<OAcceptResult> prom2 = state2.promiseState(dbId, nodeId, ODatabaseState.Online, 1L);
    assertTrue(prom2.isEmpty());

    state.setState(dbId, nodeId, ODatabaseState.Online, 1L);
    state1.setState(dbId, nodeId, ODatabaseState.Online, 1L);
    state2.setState(dbId, nodeId, ODatabaseState.Online, 1L);

    OSyncInfo syncInfo = state1.newSync(dbId).get();
    assertTrue(syncInfo.targets().contains(nodeId));
    boolean canSync = state.acceptSync(nodeId, node1, dbId, syncInfo.syncId());
    assertTrue(canSync);
    Optional<OSyncState> receiverStateOp =
        state1.canSync(nodeId, node1, dbId, syncInfo.syncId(), canSync, OSyncMode.StandardBackup);
    assertTrue(receiverStateOp.isPresent());
    OSyncState receiverState = receiverStateOp.get();

    OSyncState senderState =
        state.startSend(node1, nodeId, dbId, syncInfo.syncId(), OSyncMode.StandardBackup);

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
}
