package com.orientechnologies.orient.server.network.http;

import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.IOException;

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

  @Override
  public String getDatabaseName() {
    return "httpcommand";
  }
}
