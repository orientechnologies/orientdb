package com.orientechnologies.orient.server.distributed.impl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.OSharedContextEmbedded;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.db.record.ORecordOperation;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.record.OElement;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.transaction.ONodeId;
import com.orientechnologies.orient.core.transaction.OTransactionIdPromise;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.distributed.ODistributedRequestId;
import com.orientechnologies.orient.server.distributed.impl.task.OTransactionPhase1Task;
import com.orientechnologies.orient.server.distributed.impl.task.OTransactionPhase1TaskResult;
import com.orientechnologies.orient.server.distributed.impl.task.OTransactionPhase2Task;
import com.orientechnologies.orient.server.distributed.impl.task.transaction.OTransactionUniqueKey;
import com.orientechnologies.orient.server.distributed.impl.task.transaction.OTxRecordLockTimeout;
import com.orientechnologies.orient.server.distributed.impl.task.transaction.OTxSuccess;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;
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
        "create database ? plocal users(admin identified by 'adminpwd' role admin)",
        OTransactionPhase2TaskTest.class.getSimpleName());
    session = orientDB.open(OTransactionPhase2TaskTest.class.getSimpleName(), "admin", "adminpwd");
    session.createClass("TestClass");
  }

  @Test
  public void testOkSecondPhase() throws Exception {
    OElement element = session.save(new ODocument("TestClass"));
    ODatabaseDocumentInternal internal = (ODatabaseDocumentInternal) session;
    internal.getLocalCache().clear();
    var transactionSequence =
        ((OSharedContextEmbedded) internal.getSharedContext()).getTransactionSequence();

    ORID id = element.getIdentity();
    int pre_version = element.getVersion();
    List<ORecordOperation> operations = new ArrayList<>();
    ODocument rec1 = new ODocument(id);
    rec1.setClassName("TestClass");
    rec1.field("one", "two");
    TreeSet<ORID> ids = new TreeSet<ORID>();
    ids.add(rec1.getIdentity());
    operations.add(new ORecordOperation(rec1, ORecordOperation.UPDATED));
    SortedSet<OTransactionUniqueKey> uniqueIndexKeys = new TreeSet<>();
    OTransactionIdPromise transactionId = transactionSequence.next().get();
    OTransactionPhase1Task task =
        new OTransactionPhase1Task(operations, transactionId, new TreeSet<>());
    ODistributedRequestId firstPhaseId = new ODistributedRequestId(new ONodeId("node"), 20);
    task.execute(firstPhaseId, server, internal);
    OTransactionPhase2Task task2 =
        new OTransactionPhase2Task(firstPhaseId, true, ids, uniqueIndexKeys, transactionId);
    task2.execute(new ODistributedRequestId(new ONodeId("node"), 21), server, internal);

    assertEquals(pre_version + 1, session.load(id).getVersion());
  }

  @Test
  public void testSecondPhaseForcePromiseDespiteCompetingSuccessfulFirstPhase() throws Exception {
    ODatabaseDocumentInternal db = (ODatabaseDocumentInternal) session;
    OIdentifiable id = session.save(new ODocument("TestClass"));
    var transactionSequence =
        ((OSharedContextEmbedded) db.getSharedContext()).getTransactionSequence();
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

    ODistributedRequestId tx1p1Id = new ODistributedRequestId(new ONodeId("node"), 20);
    OTransactionIdPromise tx1Id = transactionSequence.next().get();
    OTransactionPhase1Task tx1p1 = new OTransactionPhase1Task(doc1Ops, tx1Id, doc1UniqueIndexKeys);
    OTransactionPhase1TaskResult tx1p1Result =
        (OTransactionPhase1TaskResult) tx1p1.execute(tx1p1Id, server, db);
    assertTrue(tx1p1Result.getResultPayload() instanceof OTxSuccess);

    ODistributedRequestId tx2p1Id = new ODistributedRequestId(new ONodeId("node"), 21);
    OTransactionIdPromise tx2Id = transactionSequence.next().get();
    OTransactionPhase1Task tx2p1 = new OTransactionPhase1Task(doc2Ops, tx2Id, doc2UniqueIndexKeys);
    OTransactionPhase1TaskResult tx2p1Result =
        (OTransactionPhase1TaskResult) tx2p1.execute(tx2p1Id, server, db);
    assertTrue(tx2p1Result.getResultPayload() instanceof OTxRecordLockTimeout);

    OTransactionPhase2Task tx2p2 =
        new OTransactionPhase2Task(
            tx2p1Id, true, tx2p1.getRids(), tx2p1.getUniqueKeys(), tx2p1.getPromise());
    String tx2p2Result =
        (String) tx2p2.execute(new ODistributedRequestId(new ONodeId("node"), 22), server, db);
    assertEquals(tx2p2Result, "OK");

    db.reload(doc1);
    assertEquals("2", doc1.getProperty("value"));
  }

  @After
  public void after() {
    session.close();
    server.getContext().drop(OTransactionPhase2TaskTest.class.getSimpleName());
    server.shutdown();
  }
}
