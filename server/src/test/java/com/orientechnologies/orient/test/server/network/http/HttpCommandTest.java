package com.orientechnologies.orient.test.server.network.http;

import com.orientechnologies.orient.core.record.impl.ODocument;
import java.io.IOException;
import java.io.InputStream;
import org.junit.Assert;
import org.junit.Test;

/**
 * Test HTTP "command" command.
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com) (l.garulli--at-orientdb.com)
 */
public class HttpCommandTest extends BaseHttpDatabaseTest {
  @Test
  public void commandRootCredentials() throws IOException {
    Assert.assertEquals(
        post("command/" + getDatabaseName() + "/sql/")
            .payload("select from OUSer", CONTENT.TEXT)
            .setUserName("root")
            .setUserPassword("root")
            .getResponse()
            .getStatusLine()
            .getStatusCode(),
        200);
  }

  @Test
  public void commandDatabaseCredentials() throws IOException {
    Assert.assertEquals(
        post("command/" + getDatabaseName() + "/sql/")
            .payload("select from OUSer", CONTENT.TEXT)
            .setUserName("admin")
            .setUserPassword("admin")
            .getResponse()
            .getStatusLine()
            .getStatusCode(),
        200);
  }

  @Test
  public void commandWithNamedParams() throws IOException {
    Assert.assertEquals(
        post("command/" + getDatabaseName() + "/sql/")
            .payload(
                "{\"command\":\"select from OUSer where name = :name\",\"parameters\":{\"name\":\"admin\"}}",
                CONTENT.TEXT)
            .setUserName("admin")
            .setUserPassword("admin")
            .getResponse()
            .getStatusLine()
            .getStatusCode(),
        200);

    final InputStream response = getResponse().getEntity().getContent();
    final ODocument result = new ODocument().fromJSON(response);
    final Iterable<ODocument> res = result.field("result");

    Assert.assertTrue(res.iterator().hasNext());

    final ODocument doc = res.iterator().next();
    Assert.assertEquals(doc.field("name"), "admin");
  }

  @Test
  public void commandWithPosParams() throws IOException {
    Assert.assertEquals(
        post("command/" + getDatabaseName() + "/sql/")
            .payload(
                "{\"command\":\"select from OUSer where name = ?\",\"parameters\":[\"admin\"]}",
                CONTENT.TEXT)
            .setUserName("admin")
            .setUserPassword("admin")
            .getResponse()
            .getStatusLine()
            .getStatusCode(),
        200);

    final InputStream response = getResponse().getEntity().getContent();
    final ODocument result = new ODocument().fromJSON(response);
    final Iterable<ODocument> res = result.field("result");

    Assert.assertTrue(res.iterator().hasNext());

    final ODocument doc = res.iterator().next();
    Assert.assertEquals(doc.field("name"), "admin");
  }

  @Override
  public String getDatabaseName() {
    return "httpcommand";
  }
}
