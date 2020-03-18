package com.orientechnologies.orient.server.distributed.impl;

import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.ODatabaseType;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.db.record.ORecordOperation;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.distributed.ODistributedRequestId;
import com.orientechnologies.orient.core.tx.OTransactionId;
import com.orientechnologies.orient.server.distributed.impl.task.OTransactionPhase1Task;
import com.orientechnologies.orient.server.distributed.impl.task.OTransactionPhase1TaskResult;
import com.orientechnologies.orient.server.distributed.impl.task.transaction.OTxConcurrentModification;
import com.orientechnologies.orient.server.distributed.impl.task.transaction.OTxSuccess;
import com.orientechnologies.orient.server.distributed.impl.task.transaction.OTxUniqueIndex;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import javax.management.InstanceAlreadyExistsException;
import javax.management.MBeanRegistrationException;
import javax.management.MalformedObjectNameException;
import javax.management.NotCompliantMBeanException;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;

public class OTransactionPhase1TaskTest {

  private ODatabaseSession session;
  private OServer          server;

  @Before
  public void before()
      throws ClassNotFoundException, MalformedObjectNameException, InstanceAlreadyExistsException, NotCompliantMBeanException,
      MBeanRegistrationException, InvocationTargetException, NoSuchMethodException, InstantiationException, IOException,
      IllegalAccessException {
    server = new OServer(false);
    server.startup(getClass().getClassLoader().getResourceAsStream("orientdb-server-config.xml"));
    server.activate();
    OrientDB orientDB = server.getContext();
    orientDB.create(OTransactionPhase1TaskTest.class.getSimpleName(), ODatabaseType.PLOCAL);
    session = orientDB.open(OTransactionPhase1TaskTest.class.getSimpleName(), "admin", "admin");
    session.createClass("TestClass");
    OClass clazz = session.createClass("TestClassInd");
    clazz.createProperty("one", OType.STRING).createIndex(OClass.INDEX_TYPE.UNIQUE);
  }

  @After
  public void after() {
    if (session != null)
      session.close();
    server.getContext().drop(OTransactionPhase1TaskTest.class.getSimpleName());
    server.shutdown();
  }

  @Test
  public void testExecution() throws Exception {
    OIdentifiable id = session.save(new ODocument("TestClass"));
    OIdentifiable id1 = session.save(new ODocument("TestClass"));
    session.getLocalCache().clear();

    List<ORecordOperation> operations = new ArrayList<>();
    ODocument rec1 = new ODocument(id.getIdentity());
    rec1.setClassName("TestClass");
    rec1.field("one", "two");

    ODocument rec2 = new ODocument("TestClass");
    rec2.field("one", "three");
    ((ODatabaseDocumentInternal) session).assignAndCheckCluster(rec2, null);

    operations.add(new ORecordOperation(rec1, ORecordOperation.UPDATED));
    operations.add(new ORecordOperation(id1.getIdentity(), ORecordOperation.DELETED));
    operations.add(new ORecordOperation(rec2, ORecordOperation.CREATED));

    OTransactionPhase1Task task = new OTransactionPhase1Task(operations, new OTransactionId(Optional.empty(), 0, 1));
    OTransactionPhase1TaskResult res = (OTransactionPhase1TaskResult) task
        .execute(new ODistributedRequestId(10, 20), server, null, (ODatabaseDocumentInternal) session);

    assertTrue(res.getResultPayload() instanceof OTxSuccess);
    //TODO: verify the check of the locked record if possible
  }

  @Test
  public void testExecutionConcurrentModificationUpdate() throws Exception {
    ODocument doc = new ODocument("TestClass");
    doc.field("first", "one");
    session.save(doc);
    ODocument old = doc.copy();
    doc.field("first", "two");
    session.save(doc);
    session.getLocalCache().clear();

    old.field("first", "three");
    List<ORecordOperation> operations = new ArrayList<>();
    operations.add(new ORecordOperation(old, ORecordOperation.UPDATED));
    OTransactionPhase1Task task = new OTransactionPhase1Task(operations, new OTransactionId(Optional.empty(), 0, 1));
    OTransactionPhase1TaskResult res = (OTransactionPhase1TaskResult) task
        .execute(new ODistributedRequestId(10, 20), server, null, (ODatabaseDocumentInternal) session);

    assertTrue(res.getResultPayload() instanceof OTxConcurrentModification);
    assertEquals(((OTxConcurrentModification) res.getResultPayload()).getRecordId(), old.getIdentity());
    assertEquals(((OTxConcurrentModification) res.getResultPayload()).getVersion(), doc.getVersion());
  }

  @Test
  public void testExecutionConcurrentModificationDelete() throws Exception {
    ODocument doc = new ODocument("TestClass");
    doc.field("first", "one");
    session.save(doc);
    ODocument old = doc.copy();
    doc.field("first", "two");
    session.save(doc);
    session.getLocalCache().clear();

    List<ORecordOperation> operations = new ArrayList<>();
    operations.add(new ORecordOperation(old, ORecordOperation.DELETED));

    OTransactionPhase1Task task = new OTransactionPhase1Task(operations, new OTransactionId(Optional.empty(), 0, 1));
    OTransactionPhase1TaskResult res = (OTransactionPhase1TaskResult) task
        .execute(new ODistributedRequestId(10, 20), server, null, (ODatabaseDocumentInternal) session);

    assertTrue(res.getResultPayload() instanceof OTxConcurrentModification);
    assertEquals(((OTxConcurrentModification) res.getResultPayload()).getRecordId(), old.getIdentity());
    assertEquals(((OTxConcurrentModification) res.getResultPayload()).getVersion(), doc.getVersion());
  }

  @Test
  public void testExecutionDuplicateKey() throws Exception {
    ODocument doc = new ODocument("TestClassInd");
    doc.field("one", "value");
    session.save(doc);
    ODocument doc1 = new ODocument("TestClassInd");
    ORecordInternal.setIdentity(doc1, new ORecordId(session.getClass("TestClassInd").getDefaultClusterId(), 1));
    doc1.field("one", "value");

    List<ORecordOperation> operations = new ArrayList<>();
    operations.add(new ORecordOperation(doc1, ORecordOperation.CREATED));

    OTransactionPhase1Task task = new OTransactionPhase1Task(operations, new OTransactionId(Optional.empty(), 0, 1));
    OTransactionPhase1TaskResult res = (OTransactionPhase1TaskResult) task
        .execute(new ODistributedRequestId(10, 20), server, null, (ODatabaseDocumentInternal) session);

    assertTrue(res.getResultPayload() instanceof OTxUniqueIndex);
    assertEquals(((OTxUniqueIndex) res.getResultPayload()).getRecordId(), doc.getIdentity());
  }

}
