package com.orientechnologies.orient.server;

import com.orientechnologies.orient.client.remote.OBinaryResponse;
import com.orientechnologies.orient.client.remote.message.OBeginTransactionRequest;
import com.orientechnologies.orient.client.remote.message.OBeginTransactionResponse;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.db.OrientDBFactory;
import com.orientechnologies.orient.core.db.record.ORecordOperation;
import com.orientechnologies.orient.core.record.impl.ODocument;
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

  private OrientDBFactory           factory;
  private ODatabaseDocumentInternal database;

  @Before
  public void before() {
    MockitoAnnotations.initMocks(this);
    factory = OrientDBFactory.embedded("./", OrientDBConfig.defaultConfig());
    factory.create("test", null, null, OrientDBFactory.DatabaseType.MEMORY);
    database = (ODatabaseDocumentInternal) factory.open("test", "admin", "admin");

    Mockito.when(connection.getDatabase()).thenReturn(database);
  }

  @After
  public void after() {
    database.close();
    factory.drop("test", null, null);
    factory.close();
  }

  @Test
  public void testExecutionBeginTransaction() {

    OConnectionBinaryExecutor executor = new OConnectionBinaryExecutor(connection, server);

    List<ORecordOperation> operations = new ArrayList<>();
    operations.add(new ORecordOperation(new ODocument(), ORecordOperation.CREATED));
    assertFalse(database.getTransaction().isActive());

    OBeginTransactionRequest request = new OBeginTransactionRequest(10, true, operations, new HashMap<>());
    OBinaryResponse response = request.execute(executor);
    assertTrue(database.getTransaction().isActive());
    assertTrue(response instanceof OBeginTransactionResponse);
    //TODO:Define properly what is the txId
    //assertEquals(((OBeginTransactionResponse) response).getTxId(), request.getTxId());
  }

}
