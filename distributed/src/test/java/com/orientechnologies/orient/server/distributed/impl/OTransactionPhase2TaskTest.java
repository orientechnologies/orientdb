package com.orientechnologies.orient.server.distributed.impl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.db.record.ORecordOperation;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.record.OElement;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.tx.OTransactionId;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.distributed.ODistributedRequestId;
import com.orientechnologies.orient.server.distributed.impl.task.OTransactionPhase1Task;
import com.orientechnologies.orient.server.distributed.impl.task.OTransactionPhase1TaskResult;
import com.orientechnologies.orient.server.distributed.impl.task.OTransactionPhase2Task;
import com.orientechnologies.orient.server.distributed.impl.task.transaction.OTransactionUniqueKey;
import com.orientechnologies.orient.server.distributed.impl.task.transaction.OTxRecordLockTimeout;
import com.orientechnologies.orient.server.distributed.impl.task.transaction.OTxSuccess;
import java.io.IOException;
import java.util.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class OTransactionPhase2TaskTest {

  private ODatabaseSession session;
  private OServer server;

  @Before
  public void before()
      throws ClassNotFoundException, InstantiationException, IOException, IllegalAccessException {
    server = new OServer(false);
    server.startup(getClass().getClassLoader().getResourceAsStream("orientdb-server-config.xml"));
    server.activate();
    OrientDB orientDB = server.getContext();
    orientDB.execute(
        "create database ? plocal users(admin identified by 'admin' role admin)",
        OTransactionPhase2TaskTest.class.getSimpleName());
    session = orientDB.open(OTransactionPhase2TaskTest.class.getSimpleName(), "admin", "admin");
    session.createClass("TestClass");
  }

  @Test
  public void testOkSecondPhase() throws Exception {
    OIdentifiable id = session.save(new ODocument("TestClass"));
    List<ORecordOperation> operations = new ArrayList<>();
    ODocument rec1 = new ODocument(id.getIdentity());
    rec1.setClassName("TestClass");
    rec1.field("one", "two");

    TreeSet<ORID> ids = new TreeSet<ORID>();
    ids.add(rec1.getIdentity());
    operations.add(new ORecordOperation(rec1, ORecordOperation.UPDATED));
    SortedSet<OTransactionUniqueKey> uniqueIndexKeys = new TreeSet<>();
    OTransactionId transactionId = new OTransactionId(Optional.empty(), 0, 1);
    OTransactionPhase1Task task =
        new OTransactionPhase1Task(operations, transactionId, new TreeSet<>());
    task.execute(
        new ODistributedRequestId(10, 20), server, null, (ODatabaseDocumentInternal) session);
    OTransactionPhase2Task task2 =
        new OTransactionPhase2Task(
            new ODistributedRequestId(10, 20), true, ids, uniqueIndexKeys, transactionId);
    task2.execute(
        new ODistributedRequestId(10, 21), server, null, (ODatabaseDocumentInternal) session);

    assertEquals(2, session.load(id.getIdentity()).getVersion());
  }

  @Test
  public void testSecondPhaseForcePromiseDespiteCompetingSuccessfulFirstPhase() throws Exception {
    ODatabaseDocumentInternal db = (ODatabaseDocumentInternal) session;
    OIdentifiable id = session.save(new ODocument("TestClass"));

    OElement doc1 = db.load(id.getIdentity());
    doc1.setProperty("value", "1");
    List<ORecordOperation> doc1Ops = new ArrayList<>();
    doc1Ops.add(new ORecordOperation(doc1, ORecordOperation.UPDATED));

    OElement doc2 = db.load(id.getIdentity());
    doc2.setProperty("value", "2");
    List<ORecordOperation> doc2Ops = new ArrayList<>();
    doc2Ops.add(new ORecordOperation(doc2, ORecordOperation.UPDATED));

    SortedSet<OTransactionUniqueKey> doc1UniqueIndexKeys = new TreeSet<>();
    doc1UniqueIndexKeys.add(new OTransactionUniqueKey("TestClass.value", "1", doc1.getVersion()));

    SortedSet<OTransactionUniqueKey> doc2UniqueIndexKeys = new TreeSet<>();
    doc2UniqueIndexKeys.add(new OTransactionUniqueKey("TestClass.value", "2", doc2.getVersion()));

    ODistributedRequestId tx1p1Id = new ODistributedRequestId(10, 20);
    OTransactionId tx1Id = new OTransactionId(Optional.empty(), 0, 1);
    OTransactionPhase1Task tx1p1 = new OTransactionPhase1Task(doc1Ops, tx1Id, doc1UniqueIndexKeys);
    OTransactionPhase1TaskResult tx1p1Result =
        (OTransactionPhase1TaskResult) tx1p1.execute(tx1p1Id, server, null, db);
    assertTrue(tx1p1Result.getResultPayload() instanceof OTxSuccess);

    ODistributedRequestId tx2p1Id = new ODistributedRequestId(10, 21);
    OTransactionId tx2Id = new OTransactionId(Optional.empty(), 1, 1);
    OTransactionPhase1Task tx2p1 = new OTransactionPhase1Task(doc2Ops, tx2Id, doc2UniqueIndexKeys);
    OTransactionPhase1TaskResult tx2p1Result =
        (OTransactionPhase1TaskResult) tx2p1.execute(tx2p1Id, server, null, db);
    assertTrue(tx2p1Result.getResultPayload() instanceof OTxRecordLockTimeout);

    OTransactionPhase2Task tx2p2 =
        new OTransactionPhase2Task(
            tx2p1Id, true, tx2p1.getRids(), tx2p1.getUniqueKeys(), tx2p1.getTransactionId());
    String tx2p2Result =
        (String) tx2p2.execute(new ODistributedRequestId(10, 22), server, null, db);
    assertEquals(tx2p2Result, "OK");

    doc1.reload();
    assertEquals("2", doc1.getProperty("value"));
  }

  @After
  public void after() {
    session.close();
    server.getContext().drop(OTransactionPhase2TaskTest.class.getSimpleName());
    server.shutdown();
  }
}
