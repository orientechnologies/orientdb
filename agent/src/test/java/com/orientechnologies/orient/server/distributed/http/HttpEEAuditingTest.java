package com.orientechnologies.orient.server.distributed.http;

import org.apache.http.HttpResponse;
import org.junit.Assert;
import org.junit.Test;

public class HttpEEAuditingTest extends EEBaseServerHttpTest {

  @Test
  public void getAuditingConfig() throws Exception {

    HttpResponse response = get("/auditing/" + getDatabaseName() + "/config").getResponse();

    Assert.assertEquals(200, response.getStatusLine().getStatusCode());

  }
}
