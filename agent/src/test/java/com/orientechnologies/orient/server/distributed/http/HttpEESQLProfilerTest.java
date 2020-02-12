package com.orientechnologies.orient.server.distributed.http;

import org.apache.http.HttpResponse;
import org.junit.Assert;
import org.junit.Test;

public class HttpEESQLProfilerTest extends EEBaseServerHttpTest {

  @Test
  public void getSQLProfiler() {

    HttpResponse response = get("/sqlProfiler/stats?db=" + name.getMethodName()).getResponse();

    Assert.assertEquals(200, response.getStatusLine().getStatusCode());

    response = get("/sqlProfiler/running?db=" + name.getMethodName()).getResponse();

    Assert.assertEquals(200, response.getStatusLine().getStatusCode());

    response = get("/sqlProfiler/running").getResponse();

    Assert.assertEquals(200, response.getStatusLine().getStatusCode());

  }

}
