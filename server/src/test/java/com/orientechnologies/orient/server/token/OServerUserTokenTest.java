package com.orientechnologies.orient.server.token;

import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.config.OServerUserConfiguration;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class OServerUserTokenTest {

  OServer server;

  @Before
  public void setup() throws Exception {

    server = OServer.startFromClasspathConfig("orientdb-server-config.xml");

  }

  @Test
  public void testToken() throws Exception {

    OServerUserConfiguration root = server.serverLogin("root", "root", "*");

    byte[] signedWebTokenServerUser = server.getTokenHandler().getSignedWebTokenServerUser(root);

    Assert.assertNotNull(signedWebTokenServerUser);

    JsonWebToken token = (JsonWebToken) server.getTokenHandler().parseWebToken(signedWebTokenServerUser);

    server.getTokenHandler().validateServerUserToken(token, "", "");

    Assert.assertEquals("root", token.getUserName());
    Assert.assertEquals("OrientDBServer", token.getPayload().getAudience());
  }

  @After
  public void teardown() {

    server.shutdown();
  }
}
