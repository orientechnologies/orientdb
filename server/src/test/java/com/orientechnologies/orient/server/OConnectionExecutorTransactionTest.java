package com.orientechnologies.orient.server;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.orientechnologies.orient.client.remote.OBinaryResponse;
import com.orientechnologies.orient.client.remote.message.OBatchOperationsRequest;
import com.orientechnologies.orient.client.remote.message.OBatchOperationsResponse;
import com.orientechnologies.orient.client.remote.message.OBeginTransactionRequest;
import com.orientechnologies.orient.client.remote.message.OBeginTransactionResponse;
import com.orientechnologies.orient.client.remote.message.OCommit37Request;
import com.orientechnologies.orient.client.remote.message.OCommit37Response;
import com.orientechnologies.orient.client.remote.message.OCreateRecordRequest;
import com.orientechnologies.orient.client.remote.message.OCreateRecordResponse;
import com.orientechnologies.orient.client.remote.message.OFetchTransactionRequest;
import com.orientechnologies.orient.client.remote.message.OFetchTransactionResponse;
import com.orientechnologies.orient.client.remote.message.OQueryRequest;
import com.orientechnologies.orient.client.remote.message.OQueryResponse;
import com.orientechnologies.orient.client.remote.message.ORebeginTransactionRequest;
import com.orientechnologies.orient.client.remote.message.ORollbackTransactionRequest;
import com.orientechnologies.orient.client.remote.message.OUpdateRecordRequest;
import com.orientechnologies.orient.client.remote.message.OUpdateRecordResponse;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.db.record.ORecordElement;
import com.orientechnologies.orient.core.db.record.ORecordOperation;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.serialization.serializer.record.binary.ORecordSerializerNetworkFactory;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import com.orientechnologies.orient.server.network.protocol.ONetworkProtocolData;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

/** Created by tglman on 29/12/16. */
public class OConnectionExecutorTransactionTest {

  @Mock private OServer server;
  @Mock private OClientConnection connection;

  private OrientDB orientDb;
  private ODatabaseDocumentInternal database;

  @Before
  public void before() throws IOException {
    MockitoAnnotations.initMocks(this);
    Path path =
        FileSystems.getDefault()
            .getPath("./target/" + OConnectionExecutorTransactionTest.class.getSimpleName());
    Files.createDirectories(path);
    orientDb = new OrientDB("embedded:" + path.toString(), OrientDBConfig.defaultConfig());
    orientDb.execute(
        "create database ? memory users (admin identified by 'admin' role admin)",
        OConnectionExecutorTransactionTest.class.getSimpleName());
    database =
        (ODatabaseDocumentInternal)
            orientDb.open(
                OConnectionExecutorTransactionTest.class.getSimpleName(), "admin", "admin");
    database.createClass("test");
    ONetworkProtocolData protocolData = new ONetworkProtocolData();
    protocolData.setSerializer(ORecordSerializerNetworkFactory.INSTANCE.current());
    Mockito.when(connection.getDatabase()).thenReturn(database);
    Mockito.when(connection.getData()).thenReturn(protocolData);
  }

  @After
  public void after() {
    database.close();
    orientDb.drop(OConnectionExecutorTransactionTest.class.getSimpleName());
    orientDb.close();
  }

  @Test
  public void testExecutionBeginTransaction() {

    OConnectionBinaryExecutor executor = new OConnectionBinaryExecutor(connection, server);

    List<ORecordOperation> operations = new ArrayList<>();
    ODocument rec = new ODocument();
    ORecordInternal.setIdentity(rec, new ORecordId(3, -2));
    operations.add(new ORecordOperation(rec, ORecordOperation.CREATED));
    assertFalse(database.getTransaction().isActive());

    OBeginTransactionRequest request =
        new OBeginTransactionRequest(10, true, true, operations, new HashMap<>());
    OBinaryResponse response = request.execute(executor);
    assertTrue(database.getTransaction().isActive());
    assertTrue(response instanceof OBeginTransactionResponse);
    // TODO:Define properly what is the txId
    // assertEquals(((OBeginTransactionResponse) response).getTxId(), request.getTxId());
  }

  @Test
  public void testExecutionBeginCommitTransaction() {

    OConnectionBinaryExecutor executor = new OConnectionBinaryExecutor(connection, server);

    List<ORecordOperation> operations = new ArrayList<>();
    ODocument rec = new ODocument();
    ORecordInternal.setIdentity(rec, new ORecordId(3, -2));
    operations.add(new ORecordOperation(rec, ORecordOperation.CREATED));

    assertFalse(database.getTransaction().isActive());

    OBeginTransactionRequest request =
        new OBeginTransactionRequest(10, true, true, operations, new HashMap<>());
    OBinaryResponse response = request.execute(executor);
    assertTrue(database.getTransaction().isActive());
    assertTrue(response instanceof OBeginTransactionResponse);

    OCommit37Request commit = new OCommit37Request(10, false, true, null, null);
    OBinaryResponse commitResponse = commit.execute(executor);
    assertFalse(database.getTransaction().isActive());
    assertTrue(commitResponse instanceof OCommit37Response);
    assertEquals(((OCommit37Response) commitResponse).getCreated().size(), 1);
  }

  @Test
  public void testExecutionReplaceCommitTransaction() {

    OConnectionBinaryExecutor executor = new OConnectionBinaryExecutor(connection, server);

    List<ORecordOperation> operations = new ArrayList<>();
    ODocument rec = new ODocument();
    ORecordInternal.setIdentity(rec, new ORecordId(3, -2));
    rec.setInternalStatus(ORecordElement.STATUS.LOADED);
    operations.add(new ORecordOperation(rec, ORecordOperation.CREATED));
    assertFalse(database.getTransaction().isActive());

    OBeginTransactionRequest request =
        new OBeginTransactionRequest(10, true, true, operations, new HashMap<>());
    OBinaryResponse response = request.execute(executor);
    assertTrue(database.getTransaction().isActive());
    assertTrue(response instanceof OBeginTransactionResponse);

    ODocument record1 = new ODocument(new ORecordId(3, -3));
    record1.setInternalStatus(ORecordElement.STATUS.LOADED);
    operations.add(new ORecordOperation(record1, ORecordOperation.CREATED));

    OCommit37Request commit = new OCommit37Request(10, true, true, operations, new HashMap<>());
    OBinaryResponse commitResponse = commit.execute(executor);
    assertFalse(database.getTransaction().isActive());
    assertTrue(commitResponse instanceof OCommit37Response);
    assertEquals(((OCommit37Response) commitResponse).getCreated().size(), 2);
  }

  @Test
  public void testExecutionRebeginTransaction() {

    OConnectionBinaryExecutor executor = new OConnectionBinaryExecutor(connection, server);

    List<ORecordOperation> operations = new ArrayList<>();
    ODocument rec = new ODocument();
    ORecordInternal.setIdentity(rec, new ORecordId(3, -2));
    rec.setInternalStatus(ORecordElement.STATUS.LOADED);
    operations.add(new ORecordOperation(rec, ORecordOperation.CREATED));

    assertFalse(database.getTransaction().isActive());

    OBeginTransactionRequest request =
        new OBeginTransactionRequest(10, true, true, operations, new HashMap<>());
    OBinaryResponse response = request.execute(executor);
    assertTrue(database.getTransaction().isActive());
    assertTrue(response instanceof OBeginTransactionResponse);

    ODocument record1 = new ODocument(new ORecordId(3, -3));
    record1.setInternalStatus(ORecordElement.STATUS.LOADED);
    operations.add(new ORecordOperation(record1, ORecordOperation.CREATED));

    ORebeginTransactionRequest rebegin =
        new ORebeginTransactionRequest(10, true, operations, new HashMap<>());
    OBinaryResponse rebeginResponse = rebegin.execute(executor);
    assertTrue(rebeginResponse instanceof OBeginTransactionResponse);
    assertTrue(database.getTransaction().isActive());
    assertEquals(2, database.getTransaction().getEntryCount());
  }

  @Test
  public void testExecutionRebeginCommitTransaction() {

    OConnectionBinaryExecutor executor = new OConnectionBinaryExecutor(connection, server);

    List<ORecordOperation> operations = new ArrayList<>();
    ODocument rec = new ODocument();
    ORecordInternal.setIdentity(rec, new ORecordId(3, -2));
    rec.setInternalStatus(ORecordElement.STATUS.LOADED);
    operations.add(new ORecordOperation(rec, ORecordOperation.CREATED));

    assertFalse(database.getTransaction().isActive());

    OBeginTransactionRequest request =
        new OBeginTransactionRequest(10, true, true, operations, new HashMap<>());
    OBinaryResponse response = request.execute(executor);
    assertTrue(database.getTransaction().isActive());
    assertTrue(response instanceof OBeginTransactionResponse);

    ODocument record1 = new ODocument(new ORecordId(3, -3));
    record1.setInternalStatus(ORecordElement.STATUS.LOADED);
    operations.add(new ORecordOperation(record1, ORecordOperation.CREATED));

    ORebeginTransactionRequest rebegin =
        new ORebeginTransactionRequest(10, true, operations, new HashMap<>());
    OBinaryResponse rebeginResponse = rebegin.execute(executor);
    assertTrue(rebeginResponse instanceof OBeginTransactionResponse);
    assertTrue(database.getTransaction().isActive());
    assertEquals(2, database.getTransaction().getEntryCount());

    ODocument record2 = new ODocument(new ORecordId(3, -4));
    record2.setInternalStatus(ORecordElement.STATUS.LOADED);
    operations.add(new ORecordOperation(record2, ORecordOperation.CREATED));

    OCommit37Request commit = new OCommit37Request(10, true, true, operations, new HashMap<>());
    OBinaryResponse commitResponse = commit.execute(executor);
    assertFalse(database.getTransaction().isActive());
    assertTrue(commitResponse instanceof OCommit37Response);
    assertEquals(((OCommit37Response) commitResponse).getCreated().size(), 3);
  }

  @Test
  public void testExecutionQueryChangesTracking() {

    OConnectionBinaryExecutor executor = new OConnectionBinaryExecutor(connection, server);

    List<ORecordOperation> operations = new ArrayList<>();
    ODocument rec = new ODocument("test");
    operations.add(new ORecordOperation(rec, ORecordOperation.CREATED));
    assertFalse(database.getTransaction().isActive());

    OBeginTransactionRequest request =
        new OBeginTransactionRequest(10, true, true, operations, new HashMap<>());
    OBinaryResponse response = request.execute(executor);
    assertTrue(database.getTransaction().isActive());
    assertTrue(response instanceof OBeginTransactionResponse);

    OQueryRequest query =
        new OQueryRequest(
            "sql",
            "update test set name='bla'",
            new HashMap<>(),
            OQueryRequest.COMMAND,
            ORecordSerializerNetworkFactory.INSTANCE.current(),
            20);
    OQueryResponse queryResponse = (OQueryResponse) query.execute(executor);

    assertTrue(queryResponse.isTxChanges());
  }

  @Test
  public void testBeginChangeFetchTransaction() {

    database.save(new ODocument("test"));
    OConnectionBinaryExecutor executor = new OConnectionBinaryExecutor(connection, server);

    List<ORecordOperation> operations = new ArrayList<>();
    ODocument rec = new ODocument("test");
    operations.add(new ORecordOperation(rec, ORecordOperation.CREATED));
    assertFalse(database.getTransaction().isActive());

    OBeginTransactionRequest request =
        new OBeginTransactionRequest(10, true, true, operations, new HashMap<>());
    OBinaryResponse response = request.execute(executor);
    assertTrue(database.getTransaction().isActive());
    assertTrue(response instanceof OBeginTransactionResponse);

    OQueryRequest query =
        new OQueryRequest(
            "sql",
            "update test set name='bla'",
            new HashMap<>(),
            OQueryRequest.COMMAND,
            ORecordSerializerNetworkFactory.INSTANCE.current(),
            20);
    OQueryResponse queryResponse = (OQueryResponse) query.execute(executor);

    assertTrue(queryResponse.isTxChanges());

    OFetchTransactionRequest fetchRequest = new OFetchTransactionRequest(10);

    OFetchTransactionResponse response1 =
        (OFetchTransactionResponse) fetchRequest.execute(executor);

    assertEquals(2, response1.getOperations().size());
  }

  @Test
  public void testBeginRollbackTransaction() {
    OConnectionBinaryExecutor executor = new OConnectionBinaryExecutor(connection, server);

    List<ORecordOperation> operations = new ArrayList<>();
    ODocument rec = new ODocument("test");
    operations.add(new ORecordOperation(rec, ORecordOperation.CREATED));
    assertFalse(database.getTransaction().isActive());

    OBeginTransactionRequest request =
        new OBeginTransactionRequest(10, true, true, operations, new HashMap<>());
    OBinaryResponse response = request.execute(executor);
    assertTrue(database.getTransaction().isActive());
    assertTrue(response instanceof OBeginTransactionResponse);

    ORollbackTransactionRequest rollback = new ORollbackTransactionRequest(10);
    OBinaryResponse resposne = rollback.execute(executor);
    assertFalse(database.getTransaction().isActive());
  }

  @Test
  public void testBeginBatchUpdateCommitTransaction() {

    ODocument rec = database.save(new ODocument("test").field("name", "foo"));

    OConnectionBinaryExecutor executor = new OConnectionBinaryExecutor(connection, server);

    OBeginTransactionRequest request =
        new OBeginTransactionRequest(10, true, true, new ArrayList<>(), new HashMap<>());
    OBinaryResponse response = request.execute(executor);

    assertTrue(database.getTransaction().isActive());
    assertTrue(response instanceof OBeginTransactionResponse);

    List<ORecordOperation> operations = new ArrayList<>();

    rec.field("name", "bar");

    operations.add(new ORecordOperation(rec, ORecordOperation.UPDATED));

    OBatchOperationsRequest batchRequest = new OBatchOperationsRequest(10, operations);
    OBinaryResponse batchResponse = batchRequest.execute(executor);
    assertTrue(batchResponse instanceof OBatchOperationsResponse);

    assertEquals(1, ((OBatchOperationsResponse) batchResponse).getUpdated().size());
    assertTrue(database.getTransaction().isActive());

    OCommit37Request commit = new OCommit37Request(10, false, true, null, new HashMap<>());
    OBinaryResponse commitResponse = commit.execute(executor);
    assertFalse(database.getTransaction().isActive());
    assertTrue(commitResponse instanceof OCommit37Response);

    assertEquals(((OCommit37Response) commitResponse).getUpdated().size(), 1);

    assertEquals(1, database.countClass("test"));

    ODocument document = database.browseClass("test").iterator().next();

    assertEquals("bar", document.field("name"));
  }

  @Test
  public void testBeginBatchCreateCommitTransaction() {
    OConnectionBinaryExecutor executor = new OConnectionBinaryExecutor(connection, server);

    List<ORecordOperation> operations = new ArrayList<>();
    ODocument rec = new ODocument("test");
    operations.add(new ORecordOperation(rec, ORecordOperation.CREATED));
    assertFalse(database.getTransaction().isActive());

    OBeginTransactionRequest request =
        new OBeginTransactionRequest(10, true, true, operations, new HashMap<>());
    OBinaryResponse response = request.execute(executor);

    assertTrue(database.getTransaction().isActive());
    assertTrue(response instanceof OBeginTransactionResponse);

    operations = new ArrayList<>();
    rec = new ODocument("test");
    operations.add(new ORecordOperation(rec, ORecordOperation.CREATED));

    OBatchOperationsRequest batchRequest = new OBatchOperationsRequest(10, operations);
    OBinaryResponse batchResponse = batchRequest.execute(executor);
    assertTrue(batchResponse instanceof OBatchOperationsResponse);
    assertTrue(database.getTransaction().isActive());

    assertEquals(1, ((OBatchOperationsResponse) batchResponse).getCreated().size());

    OCommit37Request commit = new OCommit37Request(10, false, true, null, new HashMap<>());
    OBinaryResponse commitResponse = commit.execute(executor);
    assertFalse(database.getTransaction().isActive());
    assertTrue(commitResponse instanceof OCommit37Response);

    assertEquals(2, ((OCommit37Response) commitResponse).getCreated().size());

    assertEquals(2, database.countClass("test"));
  }

  @Test
  public void testEmptyBeginCommitTransaction() {

    ODocument rec = database.save(new ODocument("test").field("name", "foo"));
    OConnectionBinaryExecutor executor = new OConnectionBinaryExecutor(connection, server);

    OBeginTransactionRequest request = new OBeginTransactionRequest(10, false, true, null, null);
    OBinaryResponse response = request.execute(executor);
    assertTrue(database.getTransaction().isActive());
    assertTrue(response instanceof OBeginTransactionResponse);

    OCreateRecordRequest createRecordRequest =
        new OCreateRecordRequest(
            new ODocument("test"), new ORecordId(-1, -1), ODocument.RECORD_TYPE);
    OBinaryResponse createResponse = createRecordRequest.execute(executor);
    assertTrue(createResponse instanceof OCreateRecordResponse);

    rec.setProperty("name", "bar");
    OUpdateRecordRequest updateRecordRequest =
        new OUpdateRecordRequest(
            (ORecordId) rec.getIdentity(), rec, rec.getVersion(), true, ODocument.RECORD_TYPE);
    OBinaryResponse updateResponse = updateRecordRequest.execute(executor);
    assertTrue(updateResponse instanceof OUpdateRecordResponse);

    OCommit37Request commit = new OCommit37Request(10, false, true, null, new HashMap<>());
    OBinaryResponse commitResponse = commit.execute(executor);
    assertFalse(database.getTransaction().isActive());
    assertTrue(commitResponse instanceof OCommit37Response);
    assertEquals(1, ((OCommit37Response) commitResponse).getUpdated().size());
    assertEquals(1, ((OCommit37Response) commitResponse).getCreated().size());
    assertEquals(2, database.countClass("test"));
  }

  @Test
  public void testBeginBatchDeleteCommitTransaction() {

    ODocument rec = database.save(new ODocument("test").field("name", "foo"));

    OConnectionBinaryExecutor executor = new OConnectionBinaryExecutor(connection, server);

    List<ORecordOperation> operations = new ArrayList<>();

    OBeginTransactionRequest request =
        new OBeginTransactionRequest(10, false, true, operations, new HashMap<>());
    OBinaryResponse response = request.execute(executor);

    assertTrue(database.getTransaction().isActive());
    assertTrue(response instanceof OBeginTransactionResponse);

    operations = new ArrayList<>();

    operations.add(new ORecordOperation(rec, ORecordOperation.DELETED));

    OBatchOperationsRequest batchRequest = new OBatchOperationsRequest(10, operations);
    OBinaryResponse batchResponse = batchRequest.execute(executor);
    assertTrue(batchResponse instanceof OBatchOperationsResponse);
    assertEquals(((OBatchOperationsResponse) batchResponse).getDeleted().size(), 1);
    assertEquals(
        ((OBatchOperationsResponse) batchResponse).getDeleted().get(0).getRid(), rec.getIdentity());
    assertTrue(database.getTransaction().isActive());

    OCommit37Request commit = new OCommit37Request(10, false, true, null, new HashMap<>());
    OBinaryResponse commitResponse = commit.execute(executor);
    assertFalse(database.getTransaction().isActive());
    assertTrue(commitResponse instanceof OCommit37Response);

    assertEquals(0, database.countClass("test"));
  }

  @Test
  public void testBeginBatchComplexCommitTransaction() {
    ODocument toUpdate = database.save(new ODocument("test").field("name", "foo"));
    ODocument toDelete = database.save(new ODocument("test").field("name", "delete"));

    OConnectionBinaryExecutor executor = new OConnectionBinaryExecutor(connection, server);

    List<ORecordOperation> operations = new ArrayList<>();

    OBeginTransactionRequest request =
        new OBeginTransactionRequest(10, true, true, operations, new HashMap<>());
    OBinaryResponse response = request.execute(executor);

    assertTrue(database.getTransaction().isActive());
    assertTrue(response instanceof OBeginTransactionResponse);

    operations = new ArrayList<>();
    ODocument toInsert = new ODocument("test").field("name", "insert");
    toUpdate.field("name", "update");
    operations.add(new ORecordOperation(toInsert, ORecordOperation.CREATED));
    operations.add(new ORecordOperation(toDelete, ORecordOperation.DELETED));
    operations.add(new ORecordOperation(toUpdate, ORecordOperation.UPDATED));

    OBatchOperationsRequest batchRequest = new OBatchOperationsRequest(10, operations);
    OBinaryResponse batchResponse = batchRequest.execute(executor);
    assertTrue(batchResponse instanceof OBatchOperationsResponse);
    assertTrue(database.getTransaction().isActive());

    assertEquals(1, ((OBatchOperationsResponse) batchResponse).getCreated().size());
    assertEquals(1, ((OBatchOperationsResponse) batchResponse).getUpdated().size());

    OCommit37Request commit = new OCommit37Request(10, false, true, null, new HashMap<>());
    OBinaryResponse commitResponse = commit.execute(executor);
    assertFalse(database.getTransaction().isActive());
    assertTrue(commitResponse instanceof OCommit37Response);

    assertEquals(1, ((OCommit37Response) commitResponse).getCreated().size());

    assertEquals(1, ((OCommit37Response) commitResponse).getUpdated().size());

    assertEquals(1, ((OCommit37Response) commitResponse).getDeleted().size());

    assertEquals(2, database.countClass("test"));

    OResultSet query = database.query("select from test where name = 'update'");

    List<OResult> results = query.stream().collect(Collectors.toList());

    assertEquals(1, results.size());

    assertEquals("update", results.get(0).getProperty("name"));
    query.close();
  }

  @Test
  public void testBeginSQLInsertCommitTransaction() {

    OConnectionBinaryExecutor executor = new OConnectionBinaryExecutor(connection, server);

    List<ORecordOperation> operations = new ArrayList<>();

    OBeginTransactionRequest request =
        new OBeginTransactionRequest(10, false, true, operations, new HashMap<>());
    OBinaryResponse response = request.execute(executor);

    assertTrue(database.getTransaction().isActive());
    assertTrue(response instanceof OBeginTransactionResponse);

    List<OResult> results =
        database.command("insert into test set name = 'update'").stream()
            .collect(Collectors.toList());

    assertEquals(1, results.size());

    assertEquals("update", results.get(0).getProperty("name"));

    assertTrue(results.get(0).getElement().get().getIdentity().isTemporary());

    OCommit37Request commit = new OCommit37Request(10, false, true, null, new HashMap<>());
    OBinaryResponse commitResponse = commit.execute(executor);
    assertFalse(database.getTransaction().isActive());
    assertTrue(commitResponse instanceof OCommit37Response);

    assertEquals(1, ((OCommit37Response) commitResponse).getCreated().size());

    assertTrue(
        ((OCommit37Response) commitResponse).getCreated().get(0).getCurrentRid().isTemporary());

    assertEquals(1, database.countClass("test"));

    OResultSet query = database.query("select from test where name = 'update'");

    results = query.stream().collect(Collectors.toList());

    assertEquals(1, results.size());

    assertEquals("update", results.get(0).getProperty("name"));

    query.close();
  }

  @Test
  public void testBeginBatchQueryRollbackTransaction() {

    OConnectionBinaryExecutor executor = new OConnectionBinaryExecutor(connection, server);

    ODocument test = new ODocument("test");

    ORecordInternal.setIdentity(test, -1, -2);
    // Begin Op
    List<ORecordOperation> operations = new ArrayList<>();
    operations.add(new ORecordOperation(test, ORecordOperation.CREATED));
    OBeginTransactionRequest request =
        new OBeginTransactionRequest(10, true, true, operations, new HashMap<>());
    OBinaryResponse response = request.execute(executor);

    assertTrue(database.getTransaction().isActive());
    assertTrue(response instanceof OBeginTransactionResponse);
    OBeginTransactionResponse txResponse = (OBeginTransactionResponse) response;
    assertEquals(0, txResponse.getUpdatedIds().size());

    // Batch Op

    operations = new ArrayList<>();

    test = new ODocument("test");

    ORecordInternal.setIdentity(test, -1, -3);

    operations.add(new ORecordOperation(test, ORecordOperation.CREATED));

    OBatchOperationsRequest batchRequest = new OBatchOperationsRequest(10, operations);
    OBinaryResponse response1 = batchRequest.execute(executor);
    assertTrue(response1 instanceof OBatchOperationsResponse);

    OBatchOperationsResponse batchResponse = (OBatchOperationsResponse) response1;

    assertEquals(1, batchResponse.getCreated().size());

    OResultSet query = database.query("select from test ");

    System.out.println(query.getExecutionPlan().get().prettyPrint(1, 1));

    assertEquals(2, query.stream().count());

    assertEquals(2, database.countClass("test"));

    query.close();
  }
}
