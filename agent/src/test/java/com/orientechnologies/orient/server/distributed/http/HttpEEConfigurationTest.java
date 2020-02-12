package com.orientechnologies.orient.server.distributed.http;

import org.apache.http.HttpResponse;
import org.junit.Assert;
import org.junit.Test;

public class HttpEEConfigurationTest extends EEBaseServerHttpTest {

  @Test
  public void getConfig() {

    HttpResponse response = get("/configuration").getResponse();

    Assert.assertEquals(404, response.getStatusLine().getStatusCode());

  }

}
