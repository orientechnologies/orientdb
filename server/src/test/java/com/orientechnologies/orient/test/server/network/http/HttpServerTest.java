package com.orientechnologies.orient.test.server.network.http;

import com.orientechnologies.orient.core.record.impl.ODocument;
import java.io.IOException;
import org.junit.Assert;
import org.junit.Test;

public class HttpServerTest extends BaseHttpDatabaseTest {
  @Test
  public void testGetServer() throws Exception {
    var res = get("server").getResponse();
    Assert.assertEquals(res.getCode(), 200);

    ODocument payload = new ODocument().fromJSON(res.getEntity().getContent());

    Assert.assertTrue(payload.containsField("connections"));
  }

  @Test
  public void testGetConnections() throws Exception {
    var res = setUserName("root").setUserPassword("root").get("connections/").getResponse();
    Assert.assertEquals(res.getCode(), 200);

    ODocument payload = new ODocument().fromJSON(res.getEntity().getContent());

    Assert.assertTrue(payload.containsField("connections"));
  }

  @Test
  public void testCreateDatabase() throws IOException {
    String dbName = getClass().getSimpleName() + "testCreateDatabase";
    var res =
        setUserName("root")
            .setUserPassword("root")
            .post("servercommand")
            .payload("{\"command\":\"create database " + dbName + " plocal\" }", CONTENT.JSON)
            .getResponse();
    System.out.println(res.getCode());
    Assert.assertEquals(res.getCode(), 200);

    Assert.assertTrue(getServer().getContext().exists(dbName));
    getServer().getContext().drop(dbName);
  }

  @Override
  protected String getDatabaseName() {
    return "server";
  }
}
