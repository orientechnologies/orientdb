package com.orientechnologies.orient.server;

import com.orientechnologies.orient.client.remote.OBinaryResponse;
import com.orientechnologies.orient.client.remote.message.*;
import com.orientechnologies.orient.core.db.*;
import com.orientechnologies.orient.core.db.record.ORecordOperation;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.serialization.serializer.record.binary.ORecordSerializerNetwork;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Created by tglman on 29/12/16.
 */
public class OConnetionExecutorTransactionTest {

  @Mock
  private OServer           server;
  @Mock
  private OClientConnection connection;

  private OrientDB                  orientDb;
  private ODatabaseDocumentInternal database;

  @Before
  public void before() {
    MockitoAnnotations.initMocks(this);
    orientDb = new OrientDB("embedded:./", OrientDBConfig.defaultConfig());
    orientDb.create(OConnetionExecutorTransactionTest.class.getSimpleName(), ODatabaseType.MEMORY);
    database = (ODatabaseDocumentInternal) orientDb.open(OConnetionExecutorTransactionTest.class.getSimpleName(), "admin", "admin");
    database.createClass("test");
    Mockito.when(connection.getDatabase()).thenReturn(database);
  }

  @After
  public void after() {
    database.close();
    orientDb.drop(OConnetionExecutorTransactionTest.class.getSimpleName());
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

    OBeginTransactionRequest request = new OBeginTransactionRequest(10, true, operations, new HashMap<>());
    OBinaryResponse response = request.execute(executor);
    assertTrue(database.getTransaction().isActive());
    assertTrue(response instanceof OBeginTransactionResponse);
    //TODO:Define properly what is the txId
    //assertEquals(((OBeginTransactionResponse) response).getTxId(), request.getTxId());
  }

  @Test
  public void testExecutionBeginCommitTransaction() {

    OConnectionBinaryExecutor executor = new OConnectionBinaryExecutor(connection, server);

    List<ORecordOperation> operations = new ArrayList<>();
    ODocument rec = new ODocument();
    ORecordInternal.setIdentity(rec, new ORecordId(3, -2));
    operations.add(new ORecordOperation(rec, ORecordOperation.CREATED));

    assertFalse(database.getTransaction().isActive());

    OBeginTransactionRequest request = new OBeginTransactionRequest(10, true, operations, new HashMap<>());
    OBinaryResponse response = request.execute(executor);
    assertTrue(database.getTransaction().isActive());
    assertTrue(response instanceof OBeginTransactionResponse);

    OCommit37Request commit = new OCommit37Request(10, false, true, null, null);
    OBinaryResponse commitResponse = commit.execute(executor);
    assertFalse(database.getTransaction().isActive());
    assertTrue(commitResponse instanceof OCommitResponse);
    assertEquals(((OCommitResponse) commitResponse).getCreated().size(), 1);
  }

  @Test
  public void testExecutionReplaceCommitTransaction() {

    OConnectionBinaryExecutor executor = new OConnectionBinaryExecutor(connection, server);

    List<ORecordOperation> operations = new ArrayList<>();
    ODocument rec = new ODocument();
    ORecordInternal.setIdentity(rec, new ORecordId(3, -2));
    operations.add(new ORecordOperation(rec, ORecordOperation.CREATED));
    assertFalse(database.getTransaction().isActive());

    OBeginTransactionRequest request = new OBeginTransactionRequest(10, true, operations, new HashMap<>());
    OBinaryResponse response = request.execute(executor);
    assertTrue(database.getTransaction().isActive());
    assertTrue(response instanceof OBeginTransactionResponse);

    ODocument record1 = new ODocument(new ORecordId(3, -3));
    operations.add(new ORecordOperation(record1, ORecordOperation.CREATED));

    OCommit37Request commit = new OCommit37Request(10, true, true, operations, new HashMap<>());
    OBinaryResponse commitResponse = commit.execute(executor);
    assertFalse(database.getTransaction().isActive());
    assertTrue(commitResponse instanceof OCommitResponse);
    assertEquals(((OCommitResponse) commitResponse).getCreated().size(), 2);
  }

  @Test
  public void testExecutionRebeginTransaction() {

    OConnectionBinaryExecutor executor = new OConnectionBinaryExecutor(connection, server);

    List<ORecordOperation> operations = new ArrayList<>();
    ODocument rec = new ODocument();
    ORecordInternal.setIdentity(rec, new ORecordId(3, -2));
    operations.add(new ORecordOperation(rec, ORecordOperation.CREATED));

    assertFalse(database.getTransaction().isActive());

    OBeginTransactionRequest request = new OBeginTransactionRequest(10, true, operations, new HashMap<>());
    OBinaryResponse response = request.execute(executor);
    assertTrue(database.getTransaction().isActive());
    assertTrue(response instanceof OBeginTransactionResponse);

    ODocument record1 = new ODocument(new ORecordId(3, -3));
    operations.add(new ORecordOperation(record1, ORecordOperation.CREATED));

    ORebeginTransactionRequest rebegin = new ORebeginTransactionRequest(10, true, operations, new HashMap<>());
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
    operations.add(new ORecordOperation(rec, ORecordOperation.CREATED));

    assertFalse(database.getTransaction().isActive());

    OBeginTransactionRequest request = new OBeginTransactionRequest(10, true, operations, new HashMap<>());
    OBinaryResponse response = request.execute(executor);
    assertTrue(database.getTransaction().isActive());
    assertTrue(response instanceof OBeginTransactionResponse);

    ODocument record1 = new ODocument(new ORecordId(3, -3));
    operations.add(new ORecordOperation(record1, ORecordOperation.CREATED));

    ORebeginTransactionRequest rebegin = new ORebeginTransactionRequest(10, true, operations, new HashMap<>());
    OBinaryResponse rebeginResponse = rebegin.execute(executor);
    assertTrue(rebeginResponse instanceof OBeginTransactionResponse);
    assertTrue(database.getTransaction().isActive());
    assertEquals(2, database.getTransaction().getEntryCount());

    ODocument record2 = new ODocument(new ORecordId(3, -4));
    operations.add(new ORecordOperation(record2, ORecordOperation.CREATED));

    OCommit37Request commit = new OCommit37Request(10, true, true, operations, new HashMap<>());
    OBinaryResponse commitResponse = commit.execute(executor);
    assertFalse(database.getTransaction().isActive());
    assertTrue(commitResponse instanceof OCommitResponse);
    assertEquals(((OCommitResponse) commitResponse).getCreated().size(), 3);
  }

  @Test
  public void testExecutionQueryChangesTracking() {

    OConnectionBinaryExecutor executor = new OConnectionBinaryExecutor(connection, server);

    List<ORecordOperation> operations = new ArrayList<>();
    ODocument rec = new ODocument("test");
    operations.add(new ORecordOperation(rec, ORecordOperation.CREATED));
    assertFalse(database.getTransaction().isActive());

    OBeginTransactionRequest request = new OBeginTransactionRequest(10, true, operations, new HashMap<>());
    OBinaryResponse response = request.execute(executor);
    assertTrue(database.getTransaction().isActive());
    assertTrue(response instanceof OBeginTransactionResponse);

    OQueryRequest query = new OQueryRequest("sql", "update test set name='bla'", new HashMap<>(), true,
        ORecordSerializerNetwork.INSTANCE, 20);
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

    OBeginTransactionRequest request = new OBeginTransactionRequest(10, true, operations, new HashMap<>());
    OBinaryResponse response = request.execute(executor);
    assertTrue(database.getTransaction().isActive());
    assertTrue(response instanceof OBeginTransactionResponse);

    OQueryRequest query = new OQueryRequest("sql", "update test set name='bla'", new HashMap<>(), true,
        ORecordSerializerNetwork.INSTANCE, 20);
    OQueryResponse queryResponse = (OQueryResponse) query.execute(executor);

    assertTrue(queryResponse.isTxChanges());

    OFetchTransactionRequest fetchRequest = new OFetchTransactionRequest(10);

    OFetchTransactionResponse response1 = (OFetchTransactionResponse) fetchRequest.execute(executor);

    assertEquals(2, response1.getOperations().size());

  }

  @Test
  public void testBeginRollbackTransaction() {
    OConnectionBinaryExecutor executor = new OConnectionBinaryExecutor(connection, server);

    List<ORecordOperation> operations = new ArrayList<>();
    ODocument rec = new ODocument("test");
    operations.add(new ORecordOperation(rec, ORecordOperation.CREATED));
    assertFalse(database.getTransaction().isActive());

    OBeginTransactionRequest request = new OBeginTransactionRequest(10, true, operations, new HashMap<>());
    OBinaryResponse response = request.execute(executor);
    assertTrue(database.getTransaction().isActive());
    assertTrue(response instanceof OBeginTransactionResponse);

    ORollbackTransactionRequest rollback = new ORollbackTransactionRequest(10);
    OBinaryResponse resposne = rollback.execute(executor);
    assertFalse(database.getTransaction().isActive());

  }

}
