package com.orientechnologies.orient.server.distributed.impl;

import static org.junit.Assert.assertEquals;

import com.orientechnologies.common.util.OPair;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.ODatabaseType;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.db.record.ORecordOperation;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.tx.OTransactionId;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.distributed.ODistributedRequestId;
import com.orientechnologies.orient.server.distributed.impl.task.OTransactionPhase1Task;
import com.orientechnologies.orient.server.distributed.impl.task.OTransactionPhase2Task;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.SortedSet;
import java.util.TreeSet;
import javax.management.InstanceAlreadyExistsException;
import javax.management.MBeanRegistrationException;
import javax.management.MalformedObjectNameException;
import javax.management.NotCompliantMBeanException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class OTransactionPhase2TaskTest {

  private ODatabaseSession session;
  private OServer server;

  @Before
  public void before()
      throws ClassNotFoundException, MalformedObjectNameException, InstanceAlreadyExistsException,
          NotCompliantMBeanException, MBeanRegistrationException, InvocationTargetException,
          NoSuchMethodException, InstantiationException, IOException, IllegalAccessException {
    server = new OServer(false);
    server.startup(getClass().getClassLoader().getResourceAsStream("orientdb-server-config.xml"));
    server.activate();
    OrientDB orientDB = server.getContext();
    orientDB.create(OTransactionPhase1TaskTest.class.getSimpleName(), ODatabaseType.PLOCAL);
    session = orientDB.open(OTransactionPhase1TaskTest.class.getSimpleName(), "admin", "admin");
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
    SortedSet<OPair<String, Object>> uniqueIndexKeys = new TreeSet<>();
    OTransactionId transactionId = new OTransactionId(Optional.empty(), 0, 1);
    OTransactionPhase1Task task = new OTransactionPhase1Task(operations, transactionId);
    task.execute(
        new ODistributedRequestId(10, 20), server, null, (ODatabaseDocumentInternal) session);
    OTransactionPhase2Task task2 =
        new OTransactionPhase2Task(
            new ODistributedRequestId(10, 20), true, ids, uniqueIndexKeys, transactionId);
    task2.execute(
        new ODistributedRequestId(10, 21), server, null, (ODatabaseDocumentInternal) session);

    assertEquals(2, session.load(id.getIdentity()).getVersion());
  }

  @After
  public void after() {
    session.close();
    server.getContext().drop(OTransactionPhase1TaskTest.class.getSimpleName());
    server.shutdown();
  }
}
