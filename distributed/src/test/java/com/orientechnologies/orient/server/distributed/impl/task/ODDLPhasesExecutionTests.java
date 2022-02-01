package com.orientechnologies.orient.server.distributed.impl.task;

import static org.junit.Assert.assertTrue;

import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.tx.OTransactionId;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.distributed.ODistributedRequestId;
import com.orientechnologies.orient.server.distributed.impl.ODatabaseDocumentDistributed;
import com.orientechnologies.orient.server.distributed.impl.task.transaction.OTxSuccess;
import java.io.IOException;
import java.util.Optional;
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
        "create database ? plocal users(admin identified by 'admin' role admin)",
        ODDLPhasesExecutionTests.class.getSimpleName());
  }

  @Test
  public void testExecuteFirstAndSecondPhase() throws Exception {
    OrientDB orientDB = server.getContext();
    ODatabaseDocumentDistributed session =
        (ODatabaseDocumentDistributed)
            orientDB.open(ODDLPhasesExecutionTests.class.getSimpleName(), "admin", "admin");
    String command = "create cluster bla";
    OTransactionId first = new OTransactionId(Optional.of("node"), 10, 1);
    OTransactionId second = new OTransactionId(Optional.of("node"), 30, 1);

    OSQLCommandTaskFirstPhase message = new OSQLCommandTaskFirstPhase(command, first, second);
    ODistributedRequestId requestId = new ODistributedRequestId(1, 10);
    OTransactionPhase1TaskResult result =
        (OTransactionPhase1TaskResult)
            message.execute(requestId, server, session.getDistributedManager(), session);
    assertTrue(result.getResultPayload() instanceof OTxSuccess);

    OSQLCommandTaskSecondPhase messageSecond = new OSQLCommandTaskSecondPhase(requestId, true);
    messageSecond.execute(
        new ODistributedRequestId(1, 11), server, session.getDistributedManager(), session);
    assertTrue(session.existsCluster("bla"));
    session.close();
  }

  @After
  public void after() {
    server.getContext().drop(ODDLPhasesExecutionTests.class.getSimpleName());
    server.shutdown();
  }
}
