package com.orientechnologies.orient.server.distributed.impl.coordinator.transaction;

import com.orientechnologies.orient.core.db.*;
import com.orientechnologies.orient.core.db.record.ORecordOperation;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OProperty;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.OElement;
import com.orientechnologies.orient.core.tx.OTransactionIndexChanges;
import com.orientechnologies.orient.core.tx.OTransactionOptimistic;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.distributed.impl.OIncrementOperationalLog;
import com.orientechnologies.orient.server.distributed.impl.coordinator.*;
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
import java.util.Map;
import java.util.concurrent.Executors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestTransactionFlow {

  private OrientDB orientDB;
  private OServer  server0;

  @Before
  public void before()
      throws IOException, InstantiationException, InvocationTargetException, NoSuchMethodException, MBeanRegistrationException,
      IllegalAccessException, InstanceAlreadyExistsException, NotCompliantMBeanException, ClassNotFoundException,
      MalformedObjectNameException {
    server0 = OServer.startFromClasspathConfig("orientdb-simple-dserver-config-0.xml");
    orientDB = server0.getContext();
    orientDB.create(TestTransactionFlow.class.getSimpleName(), ODatabaseType.MEMORY);
    try (ODatabaseSession session = orientDB.open(TestTransactionFlow.class.getSimpleName(), "admin", "admin")) {
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
    try (ODatabaseSession session = orientDB.open(TestTransactionFlow.class.getSimpleName(), "admin", "admin")) {
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
      submit = new OTransactionSubmit(txOps, indexes, false);
    }

    ODatabaseCoordinator coordinator = new ODatabaseCoordinator(Executors.newSingleThreadExecutor(),
        new OIncrementOperationalLog(), new ODistributedLockManagerImpl(0), new OMockAllocator());
    RecordChannel channel = new RecordChannel();
    ODistributedMember member = new ODistributedMember("one", "test", channel);
    coordinator.join(member);

    submit.begin(member, new OSessionOperationId(), coordinator);

    OTransactionFirstPhaseOperation ops = (OTransactionFirstPhaseOperation) channel.fistPhase;
    try (ODatabaseSession session = orientDB.open(TestTransactionFlow.class.getSimpleName(), "admin", "admin")) {
      ONodeResponse res = ops.execute(null, null, null, (ODatabaseDocumentInternal) session);
      assertEquals(((OTransactionFirstPhaseResult) res).getType(), OTransactionFirstPhaseResult.Type.SUCCESS);
    }
    OTransactionSecondPhaseOperation second = new OTransactionSecondPhaseOperation(ops.getOperationId(), true);
    try (ODatabaseSession session = orientDB.open(TestTransactionFlow.class.getSimpleName(), "admin", "admin")) {
      ONodeResponse res = second.execute(null, null, null, (ODatabaseDocumentInternal) session);
      assertTrue(((OTransactionSecondPhaseResponse) res).isSuccess());
    }
  }

  private static class RecordChannel implements ODistributedChannel {
    private ONodeRequest fistPhase;

    @Override
    public void submit(String database, OSessionOperationId operationId, OSubmitRequest request) {

    }

    @Override
    public void reply(String database, OSessionOperationId operationId, OSubmitResponse response) {

    }

    @Override
    public void sendRequest(String database, OLogId id, ONodeRequest nodeRequest) {
      this.fistPhase = nodeRequest;
    }

    @Override
    public void sendResponse(String database, OLogId id, ONodeResponse nodeResponse) {

    }
  }
}
