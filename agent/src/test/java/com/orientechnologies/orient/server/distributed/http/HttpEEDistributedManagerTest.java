package com.orientechnologies.orient.server.distributed.http;

import org.apache.http.HttpResponse;
import org.junit.Assert;
import org.junit.Test;

public class HttpEEDistributedManagerTest extends EEBaseDistributedHttpTest {

  @Test
  public void getTest() {

    HttpResponse response = get("/distributed/node").getResponse();

    Assert.assertEquals(200, response.getStatusLine().getStatusCode());

    response = get("/distributed/database/" + name.getMethodName()).getResponse();

    Assert.assertEquals(200, response.getStatusLine().getStatusCode());

    response = get("/distributed/stats").getResponse();

    Assert.assertEquals(200, response.getStatusLine().getStatusCode());

  }

}
