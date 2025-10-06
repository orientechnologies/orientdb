package com.orientechnologies.orient.distriubted.context;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.orientechnologies.orient.core.transaction.ODatabaseId;
import com.orientechnologies.orient.core.transaction.ONodeId;
import com.orientechnologies.orient.core.transaction.OTransactionId;
import com.orientechnologies.orient.core.transaction.OTransactionIdPromise;
import com.orientechnologies.orient.distributed.context.ODatabaseState;
import com.orientechnologies.orient.distributed.context.ODatabasesTopologyState;
import com.orientechnologies.orient.distributed.context.coordination.result.OAcceptResult;
import com.orientechnologies.orient.distributed.context.coordination.result.OAlreadyPromised;
import com.orientechnologies.orient.distributed.context.coordination.result.OInvalidSequential;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import org.junit.Test;

public class ODatabaseTopologyStateTests {

  private ONodeId newNodeId() {
    return new ONodeId(UUID.randomUUID().toString());
  }

  private ODatabaseId newDbId() {
    return new ODatabaseId(UUID.randomUUID().toString());
  }

  private OTransactionIdPromise newPromiseId() {
    return new OTransactionIdPromise(newNodeId(), new OTransactionId(10, 20));
  }

  @Test
  public void testFirstDeclare() {

    ODatabasesTopologyState state = new ODatabasesTopologyState();
    var promiseId = newPromiseId();
    Set<ONodeId> partecipants = Set.of(promiseId.getCoordinator());
    var dbId = newDbId();
    String name = "dbName";
    var res = state.promiseDeclare(promiseId, dbId, name, partecipants);
    assertTrue(res.isEmpty());

    state.declareDatabase(promiseId, dbId, name, partecipants);
    assertTrue(state.listDatabaseIds().contains(dbId));
  }

  @Test
  public void testFailDoublePromise() {

    ODatabasesTopologyState state = new ODatabasesTopologyState();

    var promiseId = newPromiseId();
    Set<ONodeId> partecipants = Set.of(promiseId.getCoordinator());
    var dbId = newDbId();
    String name = "dbName";
    var res = state.promiseDeclare(promiseId, dbId, name, partecipants);
    assertTrue(res.isEmpty());

    var promiseId1 = newPromiseId();

    res = state.promiseDeclare(promiseId1, dbId, name, partecipants);
    assertTrue(res.isPresent());
    assertTrue(res.get() instanceof OAlreadyPromised);

    var promiseId2 = newPromiseId();
    var dbId1 = newDbId();
    res = state.promiseDeclare(promiseId2, dbId1, name, partecipants);
    assertTrue(res.isPresent());
    assertTrue(res.get() instanceof OAlreadyPromised);
  }

  @Test
  public void testOkAfterCancelPromise() {

    ODatabasesTopologyState state = new ODatabasesTopologyState();

    var promiseId = newPromiseId();
    Set<ONodeId> partecipants = Set.of(promiseId.getCoordinator());
    var dbId = newDbId();
    String name = "dbName";
    var res = state.promiseDeclare(promiseId, dbId, name, partecipants);
    assertTrue(res.isEmpty());

    var promiseId1 = newPromiseId();

    res = state.promiseDeclare(promiseId1, dbId, name, partecipants);
    assertTrue(res.isPresent());
    assertTrue(res.get() instanceof OAlreadyPromised);

    state.cancelPomise(promiseId, dbId, name);

    res = state.promiseDeclare(promiseId1, dbId, name, partecipants);
    assertTrue(res.isEmpty());
  }

  @Test
  public void testSetStateBase() {

    ODatabasesTopologyState state = new ODatabasesTopologyState();

    var promiseId = newPromiseId();
    ONodeId nodeId = promiseId.getCoordinator();
    Set<ONodeId> partecipants = Set.of(nodeId);
    var dbId = newDbId();
    String name = "dbName";
    var res = state.promiseDeclare(promiseId, dbId, name, partecipants);
    assertTrue(res.isEmpty());

    state.declareDatabase(promiseId, dbId, name, partecipants);

    ODatabaseState ns = state.getNodeState(dbId, nodeId);
    assertEquals(ns, ODatabaseState.Offline);

    state.promiseState(dbId, nodeId, ODatabaseState.Online, 1L);

    state.setState(dbId, nodeId, ODatabaseState.Online, 1L);

    ns = state.getNodeState(dbId, nodeId);
    assertEquals(ns, ODatabaseState.Online);
  }

  @Test
  public void testSetStateDoublePromise() {

    ODatabasesTopologyState state = new ODatabasesTopologyState();

    var promiseId = newPromiseId();
    ONodeId nodeId = promiseId.getCoordinator();
    Set<ONodeId> partecipants = Set.of(nodeId);
    var dbId = newDbId();
    String name = "dbName";
    var res = state.promiseDeclare(promiseId, dbId, name, partecipants);
    assertTrue(res.isEmpty());

    state.declareDatabase(promiseId, dbId, name, partecipants);

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

    ODatabasesTopologyState state = new ODatabasesTopologyState();

    var promiseId = newPromiseId();
    ONodeId nodeId = promiseId.getCoordinator();
    Set<ONodeId> partecipants = Set.of(nodeId);
    var dbId = newDbId();
    String name = "dbName";
    var res = state.promiseDeclare(promiseId, dbId, name, partecipants);
    assertTrue(res.isEmpty());

    state.declareDatabase(promiseId, dbId, name, partecipants);

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

    ODatabasesTopologyState state = new ODatabasesTopologyState();

    var promiseId = newPromiseId();
    ONodeId nodeId = promiseId.getCoordinator();
    Set<ONodeId> partecipants = Set.of(nodeId);
    var dbId = newDbId();
    String name = "dbName";
    var res = state.promiseDeclare(promiseId, dbId, name, partecipants);
    assertTrue(res.isEmpty());

    state.declareDatabase(promiseId, dbId, name, partecipants);

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
}
