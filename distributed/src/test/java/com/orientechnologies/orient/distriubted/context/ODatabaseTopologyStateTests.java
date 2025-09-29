package com.orientechnologies.orient.distriubted.context;

import static org.junit.Assert.assertTrue;

import com.orientechnologies.orient.core.transaction.ODatabaseId;
import com.orientechnologies.orient.core.transaction.ONodeId;
import com.orientechnologies.orient.core.transaction.OTransactionId;
import com.orientechnologies.orient.core.transaction.OTransactionIdPromise;
import com.orientechnologies.orient.distributed.context.ODatabasesTopologyState;
import com.orientechnologies.orient.distributed.context.coordination.result.OAlreadyPromised;
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
    var dbId = newDbId();
    String name = "dbName";
    var res = state.promiseDeclare(promiseId, dbId, name);
    assertTrue(res.isEmpty());

    state.declareDatabase(promiseId, dbId, name);
    assertTrue(state.listDatabaseIds().contains(dbId));
  }

  @Test
  public void testFailDoublePromise() {

    ODatabasesTopologyState state = new ODatabasesTopologyState();

    var promiseId = newPromiseId();
    var dbId = newDbId();
    String name = "dbName";
    var res = state.promiseDeclare(promiseId, dbId, name);
    assertTrue(res.isEmpty());

    var promiseId1 = newPromiseId();

    res = state.promiseDeclare(promiseId1, dbId, name);
    assertTrue(res.isPresent());
    assertTrue(res.get() instanceof OAlreadyPromised);

    var promiseId2 = newPromiseId();
    var dbId1 = newDbId();
    res = state.promiseDeclare(promiseId2, dbId1, name);
    assertTrue(res.isPresent());
    assertTrue(res.get() instanceof OAlreadyPromised);
  }

  @Test
  public void testOkAfterCancelPromise() {

    ODatabasesTopologyState state = new ODatabasesTopologyState();

    var promiseId = newPromiseId();
    var dbId = newDbId();
    String name = "dbName";
    var res = state.promiseDeclare(promiseId, dbId, name);
    assertTrue(res.isEmpty());

    var promiseId1 = newPromiseId();

    res = state.promiseDeclare(promiseId1, dbId, name);
    assertTrue(res.isPresent());
    assertTrue(res.get() instanceof OAlreadyPromised);

    state.cancelPomise(promiseId, dbId, name);

    res = state.promiseDeclare(promiseId1, dbId, name);
    assertTrue(res.isEmpty());
  }
}
