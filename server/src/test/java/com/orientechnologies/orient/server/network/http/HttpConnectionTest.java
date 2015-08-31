package com.orientechnologies.orient.server.network.http;

import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.record.impl.ODocument;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.Collection;

/**
 * Tests HTTP "connect" command.
 * 
 * @author Luca Garulli (l.garulli--at-orientechnologies.com)
 */
@Test
public class HttpConnectionTest extends BaseHttpDatabaseTest {
  @Test
  public void testConnect() throws Exception {
    Assert.assertEquals(get("connect/" + getDatabaseName()).getResponse().getStatusLine().getStatusCode(), 204);
  }

  @Test
  public void testTooManyConnect() throws Exception {
    final int originalMax = OGlobalConfiguration.NETWORK_MAX_CONCURRENT_SESSIONS.getValueAsInteger();
    try {

      int MAX = 10;
      int TOTAL = 30;

      int good = 0;
      int bad = 0;
      OGlobalConfiguration.NETWORK_MAX_CONCURRENT_SESSIONS.setValue(MAX);
      for (int i = 0; i < TOTAL; ++i) {
        try {
          final int response = get("connect/" + getDatabaseName()).setRetry(0).getResponse().getStatusLine().getStatusCode();
          Assert.assertEquals(response, 204);
          good++;
        } catch (IOException e) {
          bad++;
        }
      }

      System.out.printf("\nTOTAL %d - MAX %d - GOOD %d - BAD %d", TOTAL, MAX, good, bad);

      Assert.assertTrue(good >= MAX);
      Assert.assertEquals(bad + good, TOTAL);

    } finally {
      OGlobalConfiguration.NETWORK_MAX_CONCURRENT_SESSIONS.setValue(originalMax);
    }
  }

  @Test
  public void testConnectAutoDisconnectKeepAlive() throws Exception {
    setKeepAlive(true);
    testConnectAutoDisconnect();
  }

  @Test
  public void testConnectAutoDisconnectNoKeepAlive() throws Exception {
    setKeepAlive(false);
    testConnectAutoDisconnect();
  }

  protected void testConnectAutoDisconnect() throws Exception {
    final int max = OGlobalConfiguration.NETWORK_MAX_CONCURRENT_SESSIONS.getValueAsInteger();

    int TOTAL = max * 3;

    for (int i = 0; i < TOTAL; ++i) {
      final int response = get("connect/" + getDatabaseName()).setRetry(0).getResponse().getStatusLine().getStatusCode();
      Assert.assertEquals(response, 204);

      if (i % 100 == 0)
        System.out.printf("\nConnections " + i);
    }

    System.out.printf("\nTest completed");

    Collection<ODocument> conns = null;
    for (int i = 0; i < 20; ++i) {
      Assert.assertEquals(get("server").setKeepAlive(false).setUserName("root").setUserPassword("root").getResponse().getStatusLine().getStatusCode(),
          200);

      final ODocument serverStatus = new ODocument().fromJSON(getResponse().getEntity().getContent());
      conns = serverStatus.field("connections");

      final int openConnections = conns.size();

      System.out.printf("\nConnections still open: " + openConnections);

      if (openConnections <= 1)
        break;

      Thread.sleep(2000);
    }

    Assert.assertTrue(conns.size() <= 1);
  }

  @Override
  public String getDatabaseName() {
    return "httpconnection";
  }
}
