package com.orientechnologies.orient.server.distributed.http;

import org.apache.http.HttpResponse;
import org.junit.Assert;
import org.junit.Test;

public class HttpEENodeTest extends EEBaseServerHttpTest {

  @Test
  public void getNodeInfo() throws Exception {

    HttpResponse response = get("/node/info").getResponse();

    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
  }

  @Test
  public void getNodeDump() throws Exception {

    HttpResponse response = get("/node/threadDump").getResponse();

    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
  }
}
