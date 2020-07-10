package com.orientechnologies.orient.server.distributed.http;

import com.orientechnologies.orient.core.record.impl.ODocument;
import org.apache.http.HttpResponse;
import org.junit.Assert;
import org.junit.Test;

public class HttpEEAuditingTest extends EEBaseServerHttpTest {

  @Test
  public void getPostAuditingConfig() throws Exception {

    HttpResponse response = get("/auditing/" + getDatabaseName() + "/config").getResponse();

    Assert.assertEquals(200, response.getStatusLine().getStatusCode());

    ODocument auditing = new ODocument().fromJSON(response.getEntity().getContent());

    ODocument backup =
        new ODocument()
            .fromJSON(
                Thread.currentThread()
                    .getContextClassLoader()
                    .getResourceAsStream("security-auditing.json"));

    response = post("/security/reload").payload(backup.toJSON(), CONTENT.JSON).getResponse();

    Assert.assertEquals(200, response.getStatusLine().getStatusCode());

    response =
        post("/auditing/" + getDatabaseName() + "/config")
            .payload(auditing.toJSON(), CONTENT.JSON)
            .getResponse();

    Assert.assertEquals(200, response.getStatusLine().getStatusCode());

    response = get("/auditing/" + getDatabaseName() + "/config").getResponse();

    Assert.assertEquals(200, response.getStatusLine().getStatusCode());

    ODocument query = new ODocument().field("db", getDatabaseName()).field("limit", 1);

    response =
        post("/auditing/" + getDatabaseName() + "/query")
            .payload(query.toJSON(), CONTENT.JSON)
            .getResponse();

    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
  }
}
