package com.orientechnologies.orient.server;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.atLeastOnce;

import com.orientechnologies.orient.core.command.OCommandResultListener;
import com.orientechnologies.orient.core.config.OContextConfiguration;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.db.record.ORecordOperation;
import com.orientechnologies.orient.core.query.live.OLiveQueryHook;
import com.orientechnologies.orient.core.query.live.OLiveQueryListener;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.serialization.serializer.record.binary.ORecordSerializerNetwork;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelBinaryServer;
import com.orientechnologies.orient.server.network.protocol.binary.OLiveCommandResultListener;
import com.orientechnologies.orient.server.network.protocol.binary.ONetworkProtocolBinary;
import com.orientechnologies.orient.server.token.OTokenHandlerImpl;
import java.io.IOException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

/** Created by tglman on 07/06/16. */
public class OLiveCommandResultListenerTest {

  @Mock private OServer server;
  @Mock private OChannelBinaryServer channelBinary;

  @Mock private OLiveQueryListener rawListener;

  private ONetworkProtocolBinary protocol;
  private OClientConnection connection;
  private ODatabaseDocumentInternal db;

  private static class TestResultListener implements OCommandResultListener {
    @Override
    public boolean result(Object iRecord) {
      return false;
    }

    @Override
    public void end() {}

    @Override
    public Object getResult() {
      return null;
    }
  }

  @Before
  public void before() throws IOException {
    MockitoAnnotations.initMocks(this);
    Mockito.when(server.getContextConfiguration()).thenReturn(new OContextConfiguration());

    db = new ODatabaseDocumentTx("memory:" + OLiveCommandResultListenerTest.class.getSimpleName());
    db.create();
    OClientConnectionManager manager = new OClientConnectionManager(server);
    protocol = new ONetworkProtocolBinary(server);
    protocol.initVariables(server, channelBinary);
    connection = manager.connect(protocol);
    OTokenHandlerImpl tokenHandler = new OTokenHandlerImpl(new OContextConfiguration());
    Mockito.when(server.getTokenHandler()).thenReturn(tokenHandler);
    byte[] token = tokenHandler.getSignedBinaryToken(db, db.getUser(), connection.getData());
    connection = manager.connect(protocol, connection, token);
    connection.setDatabase(db);
    connection.getData().setSerializationImpl(ORecordSerializerNetwork.NAME);
    Mockito.when(server.getClientConnectionManager()).thenReturn(manager);
  }

  @Test
  public void testSimpleMessageSend() throws IOException {
    OLiveCommandResultListener listener =
        new OLiveCommandResultListener(server, connection, new TestResultListener());
    ORecordOperation op = new ORecordOperation(new ODocument(), ORecordOperation.CREATED);
    listener.onLiveResult(10, op);
    Mockito.verify(channelBinary, atLeastOnce()).writeBytes(Mockito.any(byte[].class));
  }

  @Test
  public void testNetworkError() throws IOException {
    Mockito.when(channelBinary.writeInt(Mockito.anyInt()))
        .thenThrow(new IOException("Mock Exception"));
    OLiveCommandResultListener listener =
        new OLiveCommandResultListener(server, connection, new TestResultListener());
    OLiveQueryHook.subscribe(10, rawListener, db);
    assertTrue(OLiveQueryHook.getOpsReference(db).getQueueThread().hasToken(10));
    ORecordOperation op = new ORecordOperation(new ODocument(), ORecordOperation.CREATED);
    listener.onLiveResult(10, op);
    assertFalse(OLiveQueryHook.getOpsReference(db).getQueueThread().hasToken(10));
  }

  @After
  public void after() {
    db.drop();
  }
}
