package com.orientechnologies.orient.server.distributed.impl.task;

import static org.junit.Assert.assertTrue;

import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.transaction.ONodeId;
import com.orientechnologies.orient.core.transaction.OTransactionId;
import com.orientechnologies.orient.core.transaction.OTransactionIdPromise;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.distributed.ODistributedRequestId;
import com.orientechnologies.orient.server.distributed.impl.ODatabaseDocumentDistributed;
import com.orientechnologies.orient.server.distributed.impl.task.transaction.OTxSuccess;
import java.io.IOException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class ODDLPhasesExecutionTests {

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
        ODDLPhasesExecutionTests.class.getSimpleName());
  }

  @Test
  public void testExecuteFirstAndSecondPhase() throws Exception {
    OrientDB orientDB = server.getContext();
    ONodeId nodeId = new ONodeId("node");
    ODatabaseDocumentDistributed session =
        (ODatabaseDocumentDistributed)
            orientDB.open(ODDLPhasesExecutionTests.class.getSimpleName(), "admin", "adminpwd");
    String command = "create cluster bla";
    OTransactionIdPromise first = new OTransactionIdPromise(nodeId, new OTransactionId(10, 1));
    OTransactionIdPromise second = new OTransactionIdPromise(nodeId, new OTransactionId(30, 1));

    OSQLCommandTaskFirstPhase message = new OSQLCommandTaskFirstPhase(command, first, second);
    ODistributedRequestId requestId = new ODistributedRequestId(1, 10);
    OTransactionPhase1TaskResult result =
        (OTransactionPhase1TaskResult) message.execute(requestId, server, session);
    assertTrue(result.getResultPayload() instanceof OTxSuccess);

    OSQLCommandTaskSecondPhase messageSecond = new OSQLCommandTaskSecondPhase(requestId, true);
    messageSecond.execute(new ODistributedRequestId(1, 11), server, session);
    assertTrue(session.existsCluster("bla"));
    session.close();
  }

  @After
  public void after() {
    server.getContext().drop(ODDLPhasesExecutionTests.class.getSimpleName());
    server.shutdown();
  }
}
