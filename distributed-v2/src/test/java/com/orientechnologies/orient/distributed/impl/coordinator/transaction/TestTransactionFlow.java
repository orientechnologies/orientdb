package com.orientechnologies.orient.distributed.impl.coordinator.transaction;

import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.ODatabaseType;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.config.ONodeIdentity;
import com.orientechnologies.orient.core.db.record.ORecordOperation;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OProperty;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.OElement;
import com.orientechnologies.orient.core.tx.OTransactionIndexChanges;
import com.orientechnologies.orient.core.tx.OTransactionOptimistic;
import com.orientechnologies.orient.distributed.OrientDBDistributed;
import com.orientechnologies.orient.distributed.impl.coordinator.ODistributedChannel;
import com.orientechnologies.orient.distributed.impl.coordinator.ODistributedCoordinator;
import com.orientechnologies.orient.distributed.impl.coordinator.ONodeRequest;
import com.orientechnologies.orient.distributed.impl.coordinator.ONodeResponse;
import com.orientechnologies.orient.distributed.impl.coordinator.OSubmitRequest;
import com.orientechnologies.orient.distributed.impl.coordinator.OSubmitResponse;
import com.orientechnologies.orient.distributed.impl.coordinator.lock.ODistributedLockManagerImpl;
import com.orientechnologies.orient.distributed.impl.log.OIncrementOperationalLog;
import com.orientechnologies.orient.distributed.impl.log.OLogId;
import com.orientechnologies.orient.distributed.impl.structural.operations.OOperation;
import com.orientechnologies.orient.distributed.impl.structural.raft.ORaftOperation;
import com.orientechnologies.orient.distributed.impl.structural.submit.OStructuralSubmitRequest;
import com.orientechnologies.orient.distributed.impl.structural.submit.OStructuralSubmitResponse;
import com.orientechnologies.orient.server.OServer;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

@Ignore
public class TestTransactionFlow {

  private OrientDB orientDB;
  private OServer server0;

  @Before
  public void before()
      throws IOException, InstantiationException, IllegalAccessException, ClassNotFoundException {
    server0 = OServer.startFromClasspathConfig("orientdb-simple-dserver-config-0.xml");
    OrientDBDistributed impl = (OrientDBDistributed) server0.getDatabases();
    // impl.setLeader(impl.getStructuralConfiguration().getCurrentNodeIdentity(), null);
    orientDB = server0.getContext();
    orientDB.create(TestTransactionFlow.class.getSimpleName(), ODatabaseType.MEMORY);
    try (ODatabaseSession session =
        orientDB.open(TestTransactionFlow.class.getSimpleName(), "admin", "admin")) {
      OClass clazz = session.createClass("test");
      OProperty name = clazz.createProperty("name", OType.STRING);
      name.createIndex(OClass.INDEX_TYPE.UNIQUE);
    }
  }

  @After
  public void after() {
    server0.shutdown();
  }

  @Test
  public void testFlowWithIndexes() {
    Collection<ORecordOperation> txOps;
    List<OIndexOperationRequest> indexes;
    OTransactionSubmit submit;
    try (ODatabaseSession session =
        orientDB.open(TestTransactionFlow.class.getSimpleName(), "admin", "admin")) {
      session.begin();
      OElement el = session.newElement("test");
      el.setProperty("name", "john");
      session.save(el);

      OElement el1 = session.newElement("test");
      el.setProperty("link", el);
      session.save(el1);

      OTransactionOptimistic tx = (OTransactionOptimistic) session.getTransaction();
      txOps = tx.getRecordOperations();
      Map<String, OTransactionIndexChanges> indexOperations = tx.getIndexOperations();
      indexes = OTransactionSubmit.genIndexes(indexOperations, tx);
      submit = new OTransactionSubmit(txOps, indexes);
    }

    ODistributedCoordinator coordinator =
        new ODistributedCoordinator(
            Executors.newSingleThreadExecutor(),
            new OIncrementOperationalLog(),
            new ODistributedLockManagerImpl(),
            new OMockAllocator(),
            null,
            "database");
    ONodeIdentity member = new ONodeIdentity("one", "one");
    coordinator.join(member);

    submit.begin(member, new OSessionOperationId(), coordinator);
    /*
    OTransactionFirstPhaseOperation ops = (OTransactionFirstPhaseOperation) channel.fistPhase;
    try (ODatabaseSession session = orientDB.open(TestTransactionFlow.class.getSimpleName(), "admin", "admin")) {
      ONodeResponse res = ops.execute(null, null, null, (ODatabaseDocumentInternal) session);
      assertEquals(((OTransactionFirstPhaseResult) res).getType(), OTransactionFirstPhaseResult.Type.SUCCESS);
    }
    OTransactionSecondPhaseOperation second = new OTransactionSecondPhaseOperation(ops.getOperationId(), new ArrayList<>(),
        new ArrayList<>(), true);
    try (ODatabaseSession session = orientDB.open(TestTransactionFlow.class.getSimpleName(), "admin", "admin")) {
      ONodeResponse res = second.execute(null, null, null, (ODatabaseDocumentInternal) session);
      assertTrue(((OTransactionSecondPhaseResponse) res).isSuccess());
    }

     */
  }

  private static class RecordChannel implements ODistributedChannel {
    private ONodeRequest fistPhase;

    @Override
    public void submit(String database, OSessionOperationId operationId, OSubmitRequest request) {}

    @Override
    public void reply(String database, OSessionOperationId operationId, OSubmitResponse response) {}

    @Override
    public void sendRequest(String database, OLogId id, ONodeRequest nodeRequest) {
      this.fistPhase = nodeRequest;
    }

    @Override
    public void sendResponse(String database, OLogId id, ONodeResponse nodeResponse) {}

    @Override
    public void reply(OSessionOperationId operationId, OStructuralSubmitResponse response) {}

    @Override
    public void submit(OSessionOperationId operationId, OStructuralSubmitRequest request) {}

    @Override
    public void propagate(OLogId id, ORaftOperation operation) {}

    @Override
    public void ack(OLogId logId) {}

    @Override
    public void send(OOperation fullConfiguration) {}

    @Override
    public void confirm(OLogId id) {}
  }
}
