package com.orientechnologies.orient.test.server.network.http;

import com.orientechnologies.orient.core.record.impl.ODocument;
import org.apache.http.HttpResponse;
import org.junit.Assert;
import org.junit.Test;

public class HttpServerTest extends BaseHttpDatabaseTest {
  @Test
  public void testGetServer() throws Exception {
    HttpResponse res = get("server").getResponse();
    Assert.assertEquals(res.getStatusLine().getStatusCode(), 200);

    ODocument payload = new ODocument().fromJSON(res.getEntity().getContent());

    Assert.assertTrue(payload.containsField("connections"));
  }

  @Test
  public void testGetConnections() throws Exception {
    HttpResponse res = setUserName("root").setUserPassword("root").get("connections/").getResponse();
    Assert.assertEquals(res.getStatusLine().getStatusCode(), 200);

    ODocument payload = new ODocument().fromJSON(res.getEntity().getContent());

    Assert.assertTrue(payload.containsField("connections"));
  }

  @Override
  protected String getDatabaseName() {
    return "server";
  }
}
