package com.orientechnologies.orient.test.server.network.http;

import com.orientechnologies.orient.core.OConstants;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.test.server.network.http.BaseHttpTest.CONTENT;
import java.io.IOException;
import java.net.URLEncoder;
import java.util.Map;
import org.apache.http.HttpResponse;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Tests HTTP "database" command.
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com) (l.garulli--at-orientdb.com)
 */
public class HttpDatabaseTest extends BaseHttpTest {
  @Test
  public void testCreateDatabaseNoType() throws Exception {
    Assert.assertEquals(
        setUserName("root")
            .setUserPassword("root")
            .post("database/" + getDatabaseName())
            .getResponse()
            .getStatusLine()
            .getStatusCode(),
        500);
  }

  @Test
  public void testCreateDatabaseWrongPassword() throws Exception {
    Assert.assertEquals(
        setUserName("root")
            .setUserPassword("wrongPasswod")
            .post("database/wrongpasswd")
            .getResponse()
            .getStatusLine()
            .getStatusCode(),
        401);
  }

  @Test
  public void testCreateAndGetDatabase() throws IOException {

    ODocument pass = new ODocument();
    pass.setProperty("adminPassword", "admin");
    Assert.assertEquals(
        setUserName("root")
            .setUserPassword("root")
            .post("database/" + getDatabaseName() + "/memory")
            .payload(pass.toJSON(), CONTENT.JSON)
            .getResponse()
            .getStatusLine()
            .getStatusCode(),
        200);

    HttpResponse response =
        setUserName("admin")
            .setUserPassword("admin")
            .get("database/" + getDatabaseName())
            .getResponse();

    Assert.assertEquals(200, response.getStatusLine().getStatusCode());

    final ODocument payload = new ODocument().fromJSON(getResponse().getEntity().getContent());

    Map<String, Object> server = payload.field("server");
    Assert.assertEquals(OConstants.getRawVersion(), server.get("version"));
  }

  @Test
  public void testCreateQueryAndDropDatabase() throws Exception {
    ODocument pass = new ODocument();
    pass.setProperty("adminPassword", "admin");
    Assert.assertEquals(
        setUserName("root")
            .setUserPassword("root")
            .post("database/" + getDatabaseName() + "/memory")
            .payload(pass.toJSON(), CONTENT.JSON)
            .getResponse()
            .getStatusLine()
            .getStatusCode(),
        200);

    Assert.assertEquals(
        setUserName("admin")
            .setUserPassword("admin")
            .get(
                "query/"
                    + getDatabaseName()
                    + "/sql/"
                    + URLEncoder.encode("select from OUSer", "UTF8")
                    + "/10")
            .getResponse()
            .getStatusLine()
            .getStatusCode(),
        200);

    Assert.assertEquals(
        setUserName("root")
            .setUserPassword("root")
            .delete("database/" + getDatabaseName())
            .getResponse()
            .getStatusLine()
            .getStatusCode(),
        204);
  }

  @Test
  public void testDropUnknownDatabase() throws Exception {
    Assert.assertEquals(
        setUserName("root")
            .setUserPassword("root")
            .delete("database/whateverdbname")
            .getResponse()
            .getStatusLine()
            .getStatusCode(),
        500);
  }

  @Override
  public String getDatabaseName() {
    return "httpdb";
  }

  @Before
  public void startServer() throws Exception {
    super.startServer();
  }

  @After
  public void stopServer() throws Exception {
    super.stopServer();
  }
}
