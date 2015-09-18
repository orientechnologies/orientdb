package com.orientechnologies.orient.server.network.http;

import com.orientechnologies.orient.core.record.impl.ODocument;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.IOException;
import java.io.InputStream;

/**
 * Test HTTP "command" command.
 * 
 * @author Luca Garulli (l.garulli--at-orientechnologies.com)
 */
@Test
public class HttpCommandTest extends BaseHttpDatabaseTest {
  public void commandRootCredentials() throws IOException {
    Assert.assertEquals(
        post("command/" + getDatabaseName() + "/sql/").payload("select from OUSer", CONTENT.TEXT).setUserName("root")
            .setUserPassword("root").getResponse().getStatusLine().getStatusCode(), 200);
  }

  public void commandDatabaseCredentials() throws IOException {
    Assert.assertEquals(
        post("command/" + getDatabaseName() + "/sql/").payload("select from OUSer", CONTENT.TEXT).setUserName("admin")
            .setUserPassword("admin").getResponse().getStatusLine().getStatusCode(), 200);
  }

  public void commandWithNamedParams() throws IOException {
    Assert.assertEquals(
        post("command/" + getDatabaseName() + "/sql/")
            .payload("{\"command\":\"select from OUSer where name = :name\",\"parameters\":{\"name\":\"admin\"}}", CONTENT.TEXT)
            .setUserName("admin").setUserPassword("admin").getResponse().getStatusLine().getStatusCode(), 200);

    final InputStream response = getResponse().getEntity().getContent();
    final ODocument result = new ODocument().fromJSON(response);
    final Iterable<ODocument> res = result.field("result");

    Assert.assertTrue(res.iterator().hasNext());

    final ODocument doc = res.iterator().next();
    Assert.assertEquals(doc.field("name"), "admin");
  }

  public void commandWithPosParams() throws IOException {
    Assert.assertEquals(
        post("command/" + getDatabaseName() + "/sql/")
            .payload("{\"command\":\"select from OUSer where name = ?\",\"parameters\":[\"admin\"]}", CONTENT.TEXT)
            .setUserName("admin").setUserPassword("admin").getResponse().getStatusLine().getStatusCode(), 200);

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
