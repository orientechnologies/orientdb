package com.orientechnologies.orient.server.network.http;

import com.orientechnologies.orient.core.record.impl.ODocument;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.IOException;

/**
 * Test HTTP "query" command.
 * 
 * @author Luca Garulli (l.garulli--at-orientechnologies.com)
 */
@Test
public class HttpDocumentTest extends BaseHttpDatabaseTest {
  public void createDocument() throws IOException {
    post("document/" + getDatabaseName()).payload("{name:'Jay', surname:'Miner',age:99}", CONTENT.JSON).exec();

    Assert.assertEquals(getResponse().getStatusLine().getStatusCode(), 201);

    final ODocument response = new ODocument().fromJSON(getResponse().getEntity().getContent());

    Assert.assertEquals(response.field("name"), "Jay");
    Assert.assertEquals(response.field("surname"), "Miner");
    Assert.assertEquals(response.field("age"), 99);
  }

  @Override
  public String getDatabaseName() {
    return "httpdocument";
  }
}
