package com.orientechnologies.orient.server.distributed.impl.coordinator.transaction;

import com.orientechnologies.orient.client.remote.message.tx.ORecordOperationRequest;
import com.orientechnologies.orient.core.db.*;
import com.orientechnologies.orient.core.db.record.ORecordOperation;
import com.orientechnologies.orient.core.record.OElement;
import com.orientechnologies.orient.core.tx.OTransactionOptimistic;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.distributed.impl.coordinator.ONodeResponse;
import com.orientechnologies.orient.server.distributed.impl.transaction.OTransactionFirstPhaseOperation;
import com.orientechnologies.orient.server.distributed.impl.transaction.OTransactionFirstPhaseResult;
import com.orientechnologies.orient.server.distributed.impl.transaction.OTransactionSubmit;
import com.orientechnologies.orient.server.hazelcast.OHazelcastPlugin;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import javax.management.InstanceAlreadyExistsException;
import javax.management.MBeanRegistrationException;
import javax.management.MalformedObjectNameException;
import javax.management.NotCompliantMBeanException;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.Collection;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class FirstPhaseOperationTest {

  private OrientDB orientDB;
  private OServer  server;

  @Before
  public void before()
      throws IOException, InstantiationException, InvocationTargetException, NoSuchMethodException, MBeanRegistrationException,
      IllegalAccessException, InstanceAlreadyExistsException, NotCompliantMBeanException, ClassNotFoundException,
      MalformedObjectNameException {
    server = OServer.startFromClasspathConfig("orientdb-server-config.xml");
    orientDB = server.getContext();
    orientDB.create(FirstPhaseOperationTest.class.getSimpleName(), ODatabaseType.MEMORY);
    try (ODatabaseSession session = orientDB.open(FirstPhaseOperationTest.class.getSimpleName(), "admin", "admin")) {
      session.createClass("simple");
    }
  }

  @Test
  public void testExecuteSuccess() {
    List<ORecordOperationRequest> networkOps;
    try (ODatabaseSession session = orientDB.open(FirstPhaseOperationTest.class.getSimpleName(), "admin", "admin")) {
      session.begin();
      OElement ele = session.newElement("simple");
      ele.setProperty("one", "val");
      session.save(ele);
      Collection<ORecordOperation> txOps = ((OTransactionOptimistic) session.getTransaction()).getRecordOperations();
      networkOps = OTransactionSubmit.genOps(txOps);
    }

    OTransactionFirstPhaseOperation ops = new OTransactionFirstPhaseOperation(networkOps);
    try (ODatabaseSession session = orientDB.open(FirstPhaseOperationTest.class.getSimpleName(), "admin", "admin")) {
      ONodeResponse res = ops.execute(null, null, null, (ODatabaseDocumentInternal) session);
      assertEquals(((OTransactionFirstPhaseResult) res).getType(), OTransactionFirstPhaseResult.Type.SUCCESS);
    }
  }

  @After
  public void after() {
    server.shutdown();
  }

}
