package com.orientechnologies.orient.distributed.impl.coordinator.transaction;

import static org.junit.Assert.assertEquals;

import com.orientechnologies.orient.client.remote.message.tx.ORecordOperationRequest;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.ODatabaseType;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.record.ORecordOperation;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OProperty;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.OElement;
import com.orientechnologies.orient.core.tx.OTransactionIndexChanges;
import com.orientechnologies.orient.core.tx.OTransactionOptimistic;
import com.orientechnologies.orient.distributed.OrientDBDistributed;
import com.orientechnologies.orient.distributed.impl.coordinator.ONodeResponse;
import com.orientechnologies.orient.server.OServer;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

@Ignore
public class FirstPhaseOperationTest {

  private OrientDB orientDB;
  private OServer server;
  private boolean backwardCompatible;

  @Before
  public void before()
      throws IOException, InstantiationException, IllegalAccessException, ClassNotFoundException {
    backwardCompatible = OGlobalConfiguration.SERVER_BACKWARD_COMPATIBILITY.getValueAsBoolean();
    OGlobalConfiguration.SERVER_BACKWARD_COMPATIBILITY.setValue(false);
    server = OServer.startFromClasspathConfig("orientdb-server-config.xml");
    OrientDBDistributed impl = (OrientDBDistributed) server.getDatabases();
    // impl.setLeader(impl.getStructuralConfiguration().getCurrentNodeIdentity(), null);
    orientDB = server.getContext();
    orientDB.create(FirstPhaseOperationTest.class.getSimpleName(), ODatabaseType.MEMORY);
    try (ODatabaseSession session =
        orientDB.open(FirstPhaseOperationTest.class.getSimpleName(), "admin", "admin")) {
      session.createClass("simple");
    }
  }

  @Test
  public void testExecuteSuccess() {
    List<ORecordOperationRequest> networkOps;
    try (ODatabaseSession session =
        orientDB.open(FirstPhaseOperationTest.class.getSimpleName(), "admin", "admin")) {
      session.begin();
      OElement ele = session.newElement("simple");
      ele.setProperty("one", "val");
      session.save(ele);
      Collection<ORecordOperation> txOps =
          ((OTransactionOptimistic) session.getTransaction()).getRecordOperations();
      networkOps = OTransactionSubmit.genOps(txOps);
    }

    OTransactionFirstPhaseOperation ops =
        new OTransactionFirstPhaseOperation(
            new OSessionOperationId(), networkOps, new ArrayList<>());
    try (ODatabaseSession session =
        orientDB.open(FirstPhaseOperationTest.class.getSimpleName(), "admin", "admin")) {
      ONodeResponse res = ops.execute(null, null, null, (ODatabaseDocumentInternal) session);
      assertEquals(
          ((OTransactionFirstPhaseResult) res).getType(),
          OTransactionFirstPhaseResult.Type.SUCCESS);
    }
  }

  @Test
  public void testConcurrentModification() {
    List<ORecordOperationRequest> networkOps;
    ORID id;
    try (ODatabaseSession session =
        orientDB.open(FirstPhaseOperationTest.class.getSimpleName(), "admin", "admin")) {
      session.begin();
      OElement ele = session.newElement("simple");
      ele.setProperty("one", "val");
      id = session.save(ele).getIdentity();
      session.commit();
    }

    try (ODatabaseSession session =
        orientDB.open(FirstPhaseOperationTest.class.getSimpleName(), "admin", "admin")) {
      session.begin();
      OElement ele = session.load(id);
      ele.setProperty("one", "val10");
      session.save(ele);
      Collection<ORecordOperation> txOps =
          ((OTransactionOptimistic) session.getTransaction()).getRecordOperations();
      networkOps = OTransactionSubmit.genOps(txOps);
    }

    try (ODatabaseSession session =
        orientDB.open(FirstPhaseOperationTest.class.getSimpleName(), "admin", "admin")) {
      session.begin();
      OElement ele = session.load(id);
      ele.setProperty("one", "val11");
      session.save(ele);
      session.commit();
    }

    OTransactionFirstPhaseOperation ops =
        new OTransactionFirstPhaseOperation(
            new OSessionOperationId(), networkOps, new ArrayList<>());
    try (ODatabaseSession session =
        orientDB.open(FirstPhaseOperationTest.class.getSimpleName(), "admin", "admin")) {
      ONodeResponse res = ops.execute(null, null, null, (ODatabaseDocumentInternal) session);
      assertEquals(
          ((OTransactionFirstPhaseResult) res).getType(),
          OTransactionFirstPhaseResult.Type.CONCURRENT_MODIFICATION_EXCEPTION);
    }
  }

  @Test
  public void testDuplicateKey() {
    List<ORecordOperationRequest> networkOps;
    List<OIndexOperationRequest> indexes;
    try (ODatabaseSession session =
        orientDB.open(FirstPhaseOperationTest.class.getSimpleName(), "admin", "admin")) {
      OProperty pro = session.getClass("simple").createProperty("indexed", OType.STRING);
      pro.createIndex(OClass.INDEX_TYPE.UNIQUE);
      session.begin();
      OElement ele = session.newElement("simple");
      ele.setProperty("indexed", "val");
      session.save(ele);
      session.commit();
    }

    try (ODatabaseSession session =
        orientDB.open(FirstPhaseOperationTest.class.getSimpleName(), "admin", "admin")) {
      session.begin();
      OElement ele = session.newElement("simple");
      ele.setProperty("indexed", "val");
      session.save(ele);
      OTransactionOptimistic tx = (OTransactionOptimistic) session.getTransaction();
      Collection<ORecordOperation> txOps = tx.getRecordOperations();
      networkOps = OTransactionSubmit.genOps(txOps);
      Map<String, OTransactionIndexChanges> indexOperations = tx.getIndexOperations();
      indexes = OTransactionSubmit.genIndexes(indexOperations, tx);
    }

    OTransactionFirstPhaseOperation ops =
        new OTransactionFirstPhaseOperation(new OSessionOperationId(), networkOps, indexes);
    try (ODatabaseSession session =
        orientDB.open(FirstPhaseOperationTest.class.getSimpleName(), "admin", "admin")) {
      ONodeResponse res = ops.execute(null, null, null, (ODatabaseDocumentInternal) session);
      assertEquals(
          ((OTransactionFirstPhaseResult) res).getType(),
          OTransactionFirstPhaseResult.Type.UNIQUE_KEY_VIOLATION);
    }
  }

  @After
  public void after() {
    orientDB.drop(FirstPhaseOperationTest.class.getSimpleName());
    server.shutdown();
    OGlobalConfiguration.SERVER_BACKWARD_COMPATIBILITY.setValue(backwardCompatible);
  }
}
