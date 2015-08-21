package com.orientechnologies.orient.server;

import static org.testng.AssertJUnit.*;

import java.io.IOException;

import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.orientechnologies.orient.core.metadata.security.OToken;
import com.orientechnologies.orient.server.network.protocol.binary.ONetworkProtocolBinary;

public class OClientConnectionManagerTest {

  @Mock
  private ONetworkProtocolBinary protocol;

  @Mock
  private OToken token;

  @BeforeMethod
  public void before() {
    MockitoAnnotations.initMocks(this);
  }

  @Test
  public void testSimpleConnectDisconnect() throws IOException {
    OClientConnectionManager manager = new OClientConnectionManager();
    OClientConnection ret = manager.connect(protocol);
    assertNotNull(ret);
    OClientConnection ret1 = manager.getConnection(ret.id, protocol);
    assertSame(ret, ret1);
    manager.disconnect(ret);

    OClientConnection ret2 = manager.getConnection(ret.id, protocol);
    assertNull(ret2);
  }

  @Test
  public void testTokenConnectDisconnect() throws IOException {
    byte[] atoken = new byte[] {};
    Mockito.when(protocol.getTokenBytes()).thenReturn(atoken);
    OClientConnectionManager manager = new OClientConnectionManager();
    OClientConnection ret = manager.connect(protocol);
    manager.connect(protocol, ret, atoken, token);
    assertNotNull(ret);
    OClientSessions sess = manager.getSession(protocol);
    assertNotNull(sess);
    assertEquals(sess.getConnections().size(), 1);
    OClientConnection ret1 = manager.getConnection(ret.id, protocol);
    assertSame(ret, ret1);
    OClientConnection ret2 = manager.reConnect(protocol, atoken, token);
    assertNotSame(ret1, ret2);
    assertEquals(sess.getConnections().size(), 2);
    manager.disconnect(ret);

    assertEquals(sess.getConnections().size(), 1);
    OClientConnection ret3 = manager.getConnection(ret.id, protocol);
    assertNull(ret3);

    manager.disconnect(ret2);
    assertEquals(sess.getConnections().size(), 0);
    OClientConnection ret4 = manager.getConnection(ret2.id, protocol);
    assertNull(ret4);
  }

}
