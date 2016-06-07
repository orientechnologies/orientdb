package com.orientechnologies.orient.server;

import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.enterprise.channel.binary.OTokenSecurityException;
import com.orientechnologies.orient.server.network.protocol.binary.ONetworkProtocolBinary;
import com.orientechnologies.orient.server.token.OTokenHandlerImpl;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Created by tglman on 27/12/15.
 */
public class OClientConnectionTest {

  private ODatabaseDocumentInternal db;
  @Mock
  private ONetworkProtocolBinary    protocol;

  @Before
  public void before() {
    MockitoAnnotations.initMocks(this);
    db = new ODatabaseDocumentTx("memory:" + OClientConnectionTest.class.getSimpleName());
    db.create();
  }

  @After
  public void after() {
    db.drop();
  }

  @Test
  public void testValidToken() throws IOException {
    OClientConnection conn = new OClientConnection(1, null);
    OTokenHandler handler = new OTokenHandlerImpl(null);
    byte[] tokenBytes = handler.getSignedBinaryToken(db, db.getUser(), conn.getData());

    conn.validateSession(tokenBytes, handler, null);
    assertTrue(conn.getTokenBased());
    assertEquals(tokenBytes, conn.getTokenBytes());
    assertNotNull(conn.getToken());
  }

  @Test(expected = OTokenSecurityException.class)
  public void testExpiredToken() throws IOException, InterruptedException {
    OClientConnection conn = new OClientConnection(1, null);
    long sessionTimeout = OGlobalConfiguration.NETWORK_TOKEN_EXPIRE_TIMEOUT.getValueAsLong();
    OGlobalConfiguration.NETWORK_TOKEN_EXPIRE_TIMEOUT.setValue(0);
    OTokenHandler handler = new OTokenHandlerImpl(null);
    OGlobalConfiguration.NETWORK_TOKEN_EXPIRE_TIMEOUT.setValue(sessionTimeout);
    byte[] tokenBytes = handler.getSignedBinaryToken(db, db.getUser(), conn.getData());
    Thread.sleep(1);
    conn.validateSession(tokenBytes, handler, null);

  }

  @Test(expected = OTokenSecurityException.class)
  public void testWrongToken() throws IOException {
    OClientConnection conn = new OClientConnection(1, null);
    OTokenHandler handler = new OTokenHandlerImpl(null);
    byte[] tokenBytes = new byte[120];
    conn.validateSession(tokenBytes, handler, null);

  }

  @Test
  public void testAlreadyAuthenticatedOnConnection() throws IOException {
    OClientConnection conn = new OClientConnection(1, null);
    OTokenHandler handler = new OTokenHandlerImpl(null);
    byte[] tokenBytes = handler.getSignedBinaryToken(db, db.getUser(), conn.getData());
    conn.validateSession(tokenBytes, handler, protocol);
    assertTrue(conn.getTokenBased());
    assertEquals(tokenBytes, conn.getTokenBytes());
    assertNotNull(conn.getToken());
    //second validation don't need token
    conn.validateSession(null, handler, protocol);
    assertTrue(conn.getTokenBased());
    assertEquals(tokenBytes, conn.getTokenBytes());
    assertNotNull(conn.getToken());

  }

  @Test(expected = OTokenSecurityException.class)
  public void testNotAlreadyAuthenticated() throws IOException {
    OClientConnection conn = new OClientConnection(1, null);
    OTokenHandler handler = new OTokenHandlerImpl(null);
    //second validation don't need token
    conn.validateSession(null, handler, protocol);
  }

  @Test(expected = OTokenSecurityException.class)
  public void testAlreadyAuthenticatedButNotOnSpecificConnection() throws IOException {
    OClientConnection conn = new OClientConnection(1, null);
    OTokenHandler handler = new OTokenHandlerImpl(null);
    byte[] tokenBytes = handler.getSignedBinaryToken(db, db.getUser(), conn.getData());
    conn.validateSession(tokenBytes, handler, protocol);
    assertTrue(conn.getTokenBased());
    assertEquals(tokenBytes, conn.getTokenBytes());
    assertNotNull(conn.getToken());
    //second validation don't need token
    ONetworkProtocolBinary otherConn = Mockito.mock(ONetworkProtocolBinary.class);
    conn.validateSession(null, handler, otherConn);

  }

}
