package com.orientechnologies.orient.server.distributed.http;

import org.apache.http.HttpResponse;
import org.junit.Assert;
import org.junit.Test;

public class HttpEESecurityTest extends EEBaseServerHttpTest {

  @Test
  public void getSecurityConfig() {

    HttpResponse response = get("/security/config").getResponse();

    Assert.assertEquals(200, response.getStatusLine().getStatusCode());

    response = get("/security/config").payload("{ \"module\" : \"auditing\" }", CONTENT.JSON).getResponse();

    Assert.assertEquals(200, response.getStatusLine().getStatusCode());

  }

}
