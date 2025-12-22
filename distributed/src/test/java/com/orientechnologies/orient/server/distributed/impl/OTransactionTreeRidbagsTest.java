package com.orientechnologies.orient.server.distributed.impl;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;

import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBInternal;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.db.record.ORecordOperation;
import com.orientechnologies.orient.core.db.record.ridbag.ORidBag;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.tx.OTransactionId;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.distributed.ODistributedDatabase;
import com.orientechnologies.orient.server.distributed.ODistributedRequestId;
import com.orientechnologies.orient.server.distributed.ODistributedServerManager;
import com.orientechnologies.orient.server.distributed.ODistributedServerManager.DB_STATUS;
import com.orientechnologies.orient.server.distributed.impl.task.OTransactionPhase1Task;
import com.orientechnologies.orient.server.distributed.impl.task.OTransactionPhase1TaskResult;
import com.orientechnologies.orient.server.distributed.impl.task.OTransactionPhase2Task;
import com.orientechnologies.orient.server.distributed.impl.task.transaction.OTxSuccess;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class OTransactionTreeRidbagsTest {
  private ODatabaseSession session;
  private ODatabaseSession session1;
  private OServer server;
  private OIdentifiable id4;
  private OIdentifiable ridbagDoc;
  private ODatabaseSession session2;
  private int pre;

  @Before
  public void before()
      throws ClassNotFoundException, InstantiationException, IOException, IllegalAccessException,
          InterruptedException {
    pre = OGlobalConfiguration.RID_BAG_EMBEDDED_TO_SBTREEBONSAI_THRESHOLD.getValueAsInteger();
    OGlobalConfiguration.RID_BAG_EMBEDDED_TO_SBTREEBONSAI_THRESHOLD.setValue(-1);
    server = new OServer(false);
    server.startup(getClass().getClassLoader().getResourceAsStream("orientdb-server-config.xml"));
    server.activate();
    OrientDB orientDB = server.getContext();
    orientDB.execute(
        "create database ? plocal users(admin identified by 'adminpwd' role admin)",
        OTransactionTreeRidbagsTest.class.getSimpleName());
    session = orientDB.open(OTransactionTreeRidbagsTest.class.getSimpleName(), "admin", "adminpwd");
    OClass clazz = session.createClass("TestClass");
    clazz.createProperty("one", OType.LINKBAG);
    OClass clazz1 = session.createClass("ToLink");
    OIdentifiable id1 = session.save(new ODocument(clazz1));
    OIdentifiable id2 = session.save(new ODocument(clazz1));
    OIdentifiable id3 = session.save(new ODocument(clazz1));
    id4 = session.save(new ODocument(clazz1));

    ODocument doc = new ODocument(clazz);
    ORidBag bag = new ORidBag();
    bag.add(id1);
    bag.add(id2);
    bag.add(id3);

    doc.setProperty("one", bag);
    ridbagDoc = session.save(doc);

    session.backup(new FileOutputStream("target/test_sync_backup.zip"), null, null, null, 0, 4096);

    OrientDBInternal internalContext = OrientDBInternal.extract(orientDB);
    internalContext.restore(
        OTransactionTreeRidbagsTest.class.getSimpleName() + "_1",
        new FileInputStream("target/test_sync_backup.zip"),
        null,
        null,
        null);

    server
        .getDistributedManager()
        .setDatabaseStatus(
            server.getDistributedManager().getLocalNodeName(),
            OTransactionTreeRidbagsTest.class.getSimpleName() + "_1",
            DB_STATUS.ONLINE);

    session1 =
        orientDB.open(
            OTransactionTreeRidbagsTest.class.getSimpleName() + "_1", "admin", "adminpwd");

    internalContext.restore(
        OTransactionTreeRidbagsTest.class.getSimpleName() + "_2",
        new FileInputStream("target/test_sync_backup.zip"),
        null,
        null,
        null);
    server
        .getDistributedManager()
        .setDatabaseStatus(
            server.getDistributedManager().getLocalNodeName(),
            OTransactionTreeRidbagsTest.class.getSimpleName() + "_2",
            DB_STATUS.ONLINE);

    session2 =
        orientDB.open(
            OTransactionTreeRidbagsTest.class.getSimpleName() + "_2", "admin", "adminpwd");
  }

  @After
  public void after() {
    session.activateOnCurrentThread();
    if (session != null) session.close();
    server.getContext().drop(OTransactionTreeRidbagsTest.class.getSimpleName());

    session1.activateOnCurrentThread();
    if (session1 != null) session1.close();
    server.getContext().drop(OTransactionTreeRidbagsTest.class.getSimpleName() + "_1");

    session2.activateOnCurrentThread();
    if (session2 != null) session2.close();
    server.getContext().drop(OTransactionTreeRidbagsTest.class.getSimpleName() + "_2");

    server.shutdown();
    OGlobalConfiguration.RID_BAG_EMBEDDED_TO_SBTREEBONSAI_THRESHOLD.setValue(pre);
  }

  private void firstPhaseExecution(
      OTransactionPhase1Task task, ODistributedRequestId requestId, ODatabaseSession session)
      throws Exception {
    task = passNetwork(task);
    OTransactionPhase1TaskResult res =
        (OTransactionPhase1TaskResult)
            task.execute(requestId, server, null, (ODatabaseDocumentInternal) session);

    assertTrue(res.getResultPayload().toString(), res.getResultPayload() instanceof OTxSuccess);
  }

  private OTransactionPhase1Task createFirstPhase(ODocument doc, ODatabaseSession session) {
    List<ORecordOperation> operations = new ArrayList<>();
    operations.add(new ORecordOperation(doc, ORecordOperation.UPDATED));
    ODistributedServerManager dm = server.getDistributedManager();
    ODistributedDatabase dd = dm.getDatabase(session.getName());
    OTransactionId txId = dd.nextId().get();
    dd.rollback(txId);
    return new OTransactionPhase1Task(operations, txId, new TreeSet<>());
  }

  private OTransactionPhase1Task passNetwork(OTransactionPhase1Task task) throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    DataOutputStream dataOut = new DataOutputStream(out);
    task.toStream(dataOut);
    OTransactionPhase1Task tx = new OTransactionPhase1Task();
    tx.fromStream(new DataInputStream(new ByteArrayInputStream(out.toByteArray())), null);
    return tx;
  }

  @Test
  public void testExecution() throws Exception {

    session.activateOnCurrentThread();

    ODocument doc = session.load(ridbagDoc.getIdentity());
    ((ORidBag) doc.getProperty("one")).add(id4);

    OTransactionPhase1Task task = createFirstPhase(doc, session);
    session.getLocalCache().clear();
    // Start the first transaction that do not update the version and reach the quorum of two nodes.
    ODistributedRequestId requestIdTx = new ODistributedRequestId(10, 20);
    firstPhaseExecution(task, requestIdTx, session);

    session2.activateOnCurrentThread();
    firstPhaseExecution(task, requestIdTx, session2);

    SortedSet<ORID> ids = new TreeSet<>();
    ids.add(ridbagDoc.getIdentity());
    OTransactionPhase2Task secondPhase =
        new OTransactionPhase2Task(
            requestIdTx, true, ids, new TreeSet<>(), task.getTransactionId());

    session.activateOnCurrentThread();
    secondPhase.execute(
        new ODistributedRequestId(10, 21), server, null, (ODatabaseDocumentInternal) session);
    session2.activateOnCurrentThread();
    secondPhase.execute(
        new ODistributedRequestId(10, 21), server, null, (ODatabaseDocumentInternal) session2);

    // Applied the first transaction to two nodes

    // Start the second transaction that update the version and is applied to all nodesnodes.

    session2.getLocalCache().clear();
    ODocument doc1 = session2.load(ridbagDoc.getIdentity());
    doc1.setProperty("two", "value");

    OTransactionPhase1Task task1 = createFirstPhase(doc1, session2);

    ODistributedRequestId requestIdTx1 = new ODistributedRequestId(11, 20);

    firstPhaseExecution(task1, requestIdTx1, session2);

    session.activateOnCurrentThread();
    session.getLocalCache().clear();
    firstPhaseExecution(task1, requestIdTx1, session);

    session1.activateOnCurrentThread();
    session1.getLocalCache().clear();
    firstPhaseExecution(task1, requestIdTx1, session1);

    OTransactionPhase2Task secondPhase1 =
        new OTransactionPhase2Task(
            requestIdTx1, true, ids, new TreeSet<>(), task1.getTransactionId());
    secondPhase1.execute(
        new ODistributedRequestId(11, 21), server, null, (ODatabaseDocumentInternal) session1);

    session.activateOnCurrentThread();
    secondPhase1.execute(
        new ODistributedRequestId(11, 21), server, null, (ODatabaseDocumentInternal) session);

    session2.activateOnCurrentThread();
    secondPhase1.execute(
        new ODistributedRequestId(11, 21), server, null, (ODatabaseDocumentInternal) session2);

    // Completed the second transaction to all nodes.

    // Execute the two phases of the first transaction on the missing node
    session1.activateOnCurrentThread();

    firstPhaseExecution(task, requestIdTx, session1);
    secondPhase.execute(
        new ODistributedRequestId(10, 21), server, null, (ODatabaseDocumentInternal) session1);

    assertContent(session);
    assertContent(session1);
    assertContent(session2);
  }

  private void assertContent(ODatabaseSession session) {
    session.activateOnCurrentThread();
    session.getLocalCache().clear();
    ODocument docRead = session.load(ridbagDoc.getIdentity());
    assertTrue(((ORidBag) docRead.getProperty("one")).contains(id4));
    assertEquals((String) docRead.getProperty("two"), "value");
  }
}
