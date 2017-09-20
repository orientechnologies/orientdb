package com.orientechnologies.orient.server.distributed.impl;

import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.db.*;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.db.record.ORecordOperation;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.distributed.ODistributedRequestId;
import com.orientechnologies.orient.server.distributed.impl.task.OTransactionPhase1Task;
import com.orientechnologies.orient.server.distributed.impl.task.OTransactionPhase1TaskResult;
import com.orientechnologies.orient.server.distributed.impl.task.transaction.OTxSuccess;
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

import static junit.framework.TestCase.assertTrue;

public class OTransactionPhase1TaskTest {

  private ODatabaseSession session;
  private OServer          server;

  @Before
  public void before()
      throws ClassNotFoundException, MalformedObjectNameException, InstanceAlreadyExistsException, NotCompliantMBeanException,
      MBeanRegistrationException, InvocationTargetException, NoSuchMethodException, InstantiationException, IOException,
      IllegalAccessException {
    server = new OServer();
    server.startup(getClass().getClassLoader().getResourceAsStream("orientdb-server-config.xml"));
    server.activate();
    OrientDB orientDB = server.getContext();
    orientDB.create(OTransactionPhase1TaskTest.class.getSimpleName(), ODatabaseType.PLOCAL);
    session = orientDB.open(OTransactionPhase1TaskTest.class.getSimpleName(), "admin", "admin");
    session.createClass("TestClass");
  }

  @After
  public void after() {
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

    operations.add(new ORecordOperation(rec1, ORecordOperation.UPDATED));
    operations.add(new ORecordOperation(id1.getIdentity(), ORecordOperation.DELETED));
    operations.add(new ORecordOperation(rec2, ORecordOperation.DELETED));

    OTransactionPhase1Task task = new OTransactionPhase1Task(operations);
    OTransactionPhase1TaskResult res = (OTransactionPhase1TaskResult) task
        .execute(new ODistributedRequestId(10, 20), server, null, (ODatabaseDocumentInternal) session);

    assertTrue(res.getResultPayload() instanceof OTxSuccess);
    //TODO: verify the check of the locked record if possible
  }

}
