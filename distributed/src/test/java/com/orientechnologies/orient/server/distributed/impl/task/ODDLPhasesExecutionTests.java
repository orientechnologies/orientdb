package com.orientechnologies.orient.server.distributed.impl.task;

import static org.junit.Assert.assertTrue;

import com.orientechnologies.common.util.ORawPair;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.id.ONodeId;
import com.orientechnologies.orient.core.transaction.OTransactionIdPromise;
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
    var txs = session.getSharedContext().getTransactionSequence();
    Optional<ORawPair<OTransactionIdPromise, OTransactionIdPromise>> ids;
    do {
      ids = txs.nextDDL();
      if (ids.isEmpty()) {
        Thread.sleep(10);
      }
    } while (ids.isEmpty());

    OTransactionIdPromise first = ids.get().first;
    OTransactionIdPromise second = ids.get().second;

    OSQLCommandTaskFirstPhase message = new OSQLCommandTaskFirstPhase(command, first, second);
    ODistributedRequestId requestId = new ODistributedRequestId(nodeId, 10);
    OTransactionPhase1TaskResult result =
        (OTransactionPhase1TaskResult) message.execute(requestId, server, session);
    assertTrue(
        result.getResultPayload().toString(), result.getResultPayload() instanceof OTxSuccess);

    OSQLCommandTaskSecondPhase messageSecond =
        new OSQLCommandTaskSecondPhase(requestId, first, second, true);
    messageSecond.execute(new ODistributedRequestId(nodeId, 11), server, session);
    assertTrue(session.existsCluster("bla"));
    session.close();
  }

  @After
  public void after() {
    server.getContext().drop(ODDLPhasesExecutionTests.class.getSimpleName());
    server.shutdown();
  }
}
