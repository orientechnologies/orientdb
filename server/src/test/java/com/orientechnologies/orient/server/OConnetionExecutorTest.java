package com.orientechnologies.orient.server;

import com.orientechnologies.orient.client.remote.OBinaryResponse;
import com.orientechnologies.orient.client.remote.message.OBatchOperationsRequest;
import com.orientechnologies.orient.client.remote.message.OBatchOperationsResponse;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.ODatabaseType;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.db.record.ORecordOperation;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.serialization.serializer.record.binary.ORecordSerializerNetworkV37;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import com.orientechnologies.orient.server.network.protocol.ONetworkProtocolData;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Created by tglman on 29/12/16.
 */
public class OConnetionExecutorTest {

  @Mock
  private OServer           server;
  @Mock
  private OClientConnection connection;

  @Mock
  private ONetworkProtocolData data;

  private OrientDB                  orientDb;
  private ODatabaseDocumentInternal database;

  @Before
  public void before() {
    MockitoAnnotations.initMocks(this);
    orientDb = new OrientDB("embedded:./", OrientDBConfig.defaultConfig());
    orientDb.create(OConnetionExecutorTest.class.getSimpleName(), ODatabaseType.MEMORY);
    database = (ODatabaseDocumentInternal) orientDb.open(OConnetionExecutorTest.class.getSimpleName(), "admin", "admin");
    database.createClass("test");
    Mockito.when(connection.getDatabase()).thenReturn(database);
    Mockito.when(connection.getData()).thenReturn(data);
    Mockito.when(data.getSerializer()).thenReturn(ORecordSerializerNetworkV37.INSTANCE);
  }

  @After
  public void after() {
    database.close();
    orientDb.drop(OConnetionExecutorTest.class.getSimpleName());
    orientDb.close();
  }

  @Test
  public void testBatchOperationsNoTX() {
    ODocument toUpdate = database.save(new ODocument("test").field("name", "foo"));
    ODocument toDelete = database.save(new ODocument("test").field("name", "delete"));

    OConnectionBinaryExecutor executor = new OConnectionBinaryExecutor(connection, server);

    List<ORecordOperation> operations = new ArrayList<>();

    ODocument toInsert = new ODocument("test").field("name", "insert");
    toUpdate.field("name", "update");
    operations.add(new ORecordOperation(toInsert, ORecordOperation.CREATED));
    operations.add(new ORecordOperation(toDelete, ORecordOperation.DELETED));
    operations.add(new ORecordOperation(toUpdate, ORecordOperation.UPDATED));

    OBatchOperationsRequest batchRequest = new OBatchOperationsRequest(10, operations);
    OBinaryResponse batchResponse = batchRequest.execute(executor);
    assertTrue(batchResponse instanceof OBatchOperationsResponse);
    assertFalse(database.getTransaction().isActive());

    assertEquals(1, ((OBatchOperationsResponse) batchResponse).getCreated().size());
    assertEquals(1, ((OBatchOperationsResponse) batchResponse).getUpdated().size());

    assertEquals(2, database.countClass("test"));

    OResultSet query = database.query("select from test where name = 'update'");

    List<OResult> results = query.stream().collect(Collectors.toList());

    assertEquals(1, results.size());

    assertEquals("update", results.get(0).getProperty("name"));
    query.close();
  }
}
