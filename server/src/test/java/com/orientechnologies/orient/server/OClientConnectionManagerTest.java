package com.orientechnologies.orient.server;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;

import com.orientechnologies.orient.core.metadata.security.OToken;
import com.orientechnologies.orient.core.security.OParsedToken;
import com.orientechnologies.orient.server.network.protocol.binary.ONetworkProtocolBinary;
import java.io.IOException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

public class OClientConnectionManagerTest {

  @Mock private ONetworkProtocolBinary protocol;

  @Mock private OToken token;

  @Mock private OTokenHandler handler;

  @Mock private OServer server;

  @Before
  public void before() throws NoSuchAlgorithmException, InvalidKeyException, IOException {
    MockitoAnnotations.initMocks(this);
    Mockito.when(handler.parseBinaryToken(Mockito.any(byte[].class))).thenReturn(token);
    Mockito.when(handler.validateBinaryToken(Mockito.any(OToken.class))).thenReturn(true);
    Mockito.when(handler.validateBinaryToken(Mockito.any(OParsedToken.class))).thenReturn(true);
    Mockito.when(protocol.getServer()).thenReturn(server);
    Mockito.when(server.getTokenHandler()).thenReturn(handler);
  }

  @Test
  public void testSimpleConnectDisconnect() throws IOException {
    OClientConnectionManager manager = new OClientConnectionManager(server);
    OClientConnection ret = manager.connect(protocol);
    assertNotNull(ret);
    OClientConnection ret1 = manager.getConnection(ret.getId(), protocol);
    assertSame(ret, ret1);
    manager.disconnect(ret);

    OClientConnection ret2 = manager.getConnection(ret.getId(), protocol);
    assertNull(ret2);
  }

  @Test
  @Ignore
  public void testTokenConnectDisconnect() throws IOException {
    byte[] atoken = new byte[] {};

    OClientConnectionManager manager = new OClientConnectionManager(server);
    OClientConnection ret = manager.connect(protocol);
    manager.connect(protocol, ret, atoken);
    assertNotNull(ret);
    OClientSessions sess = manager.getSession(ret);
    assertNotNull(sess);
    assertEquals(sess.getConnections().size(), 1);
    OClientConnection ret1 = manager.getConnection(ret.getId(), protocol);
    assertSame(ret, ret1);
    OClientConnection ret2 = manager.reConnect(protocol, atoken);
    assertNotSame(ret1, ret2);
    assertEquals(sess.getConnections().size(), 2);
    manager.disconnect(ret);

    assertEquals(sess.getConnections().size(), 1);
    OClientConnection ret3 = manager.getConnection(ret.getId(), protocol);
    assertNull(ret3);

    manager.disconnect(ret2);
    assertEquals(sess.getConnections().size(), 0);
    OClientConnection ret4 = manager.getConnection(ret2.getId(), protocol);
    assertNull(ret4);
  }
}
