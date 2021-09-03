package com.orientechnologies.orient.test.server.network.http;

import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.record.impl.ODocument;
import java.io.IOException;
import java.util.Collection;
import org.junit.Assert;
import org.junit.Test;

/**
 * Tests HTTP "connect" command.
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com) (l.garulli--at-orientdb.com)
 */
public class HttpConnectionTest extends BaseHttpDatabaseTest {
  @Test
  public void testConnect() throws Exception {
    Assert.assertEquals(
        get("connect/" + getDatabaseName()).getResponse().getStatusLine().getStatusCode(), 204);
  }

  public void testTooManyConnect() throws Exception {
    if (isInDevelopmentMode())
      // SKIP IT
      return;

    final int originalMax =
        OGlobalConfiguration.NETWORK_MAX_CONCURRENT_SESSIONS.getValueAsInteger();
    try {

      int MAX = 10;
      int TOTAL = 30;

      int good = 0;
      int bad = 0;
      OGlobalConfiguration.NETWORK_MAX_CONCURRENT_SESSIONS.setValue(MAX);
      for (int i = 0; i < TOTAL; ++i) {
        try {
          final int response =
              get("connect/" + getDatabaseName())
                  .setRetry(0)
                  .getResponse()
                  .getStatusLine()
                  .getStatusCode();
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

  public void testConnectAutoDisconnectKeepAlive() throws Exception {
    setKeepAlive(true);
    testConnectAutoDisconnect();
  }

  public void testConnectAutoDisconnectNoKeepAlive() throws Exception {
    setKeepAlive(false);
    testConnectAutoDisconnect();
  }

  protected void testConnectAutoDisconnect() throws Exception {
    if (isInDevelopmentMode())
      // SKIP IT
      return;

    final int max = OGlobalConfiguration.NETWORK_MAX_CONCURRENT_SESSIONS.getValueAsInteger();

    int TOTAL = max * 3;

    for (int i = 0; i < TOTAL; ++i) {
      final int response =
          get("connect/" + getDatabaseName())
              .setRetry(0)
              .getResponse()
              .getStatusLine()
              .getStatusCode();
      Assert.assertEquals(response, 204);

      if (i % 100 == 0) System.out.printf("\nConnections " + i);
    }

    System.out.printf("\nTest completed");

    Collection<ODocument> conns = null;
    for (int i = 0; i < 20; ++i) {
      Assert.assertEquals(
          get("server")
              .setKeepAlive(false)
              .setUserName("root")
              .setUserPassword("root")
              .getResponse()
              .getStatusLine()
              .getStatusCode(),
          200);

      final ODocument serverStatus =
          new ODocument().fromJSON(getResponse().getEntity().getContent());
      conns = serverStatus.field("connections");

      final int openConnections = conns.size();

      System.out.printf("\nConnections still open: " + openConnections);

      if (openConnections <= 1) break;

      Thread.sleep(1000);
    }

    System.out.printf("\nOK: connections: " + conns.size());

    Assert.assertTrue(conns.size() <= 1);
  }

  @Override
  public String getDatabaseName() {
    return "httpconnection";
  }
}
