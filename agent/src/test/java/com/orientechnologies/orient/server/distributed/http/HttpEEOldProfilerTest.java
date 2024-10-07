package com.orientechnologies.orient.server.distributed.http;

import org.apache.http.HttpResponse;
import org.junit.Assert;
import org.junit.Test;

public class HttpEEOldProfilerTest extends EEBaseServerHttpTest {

  @Test
  public void getProfilerRealtime() throws Exception {

    HttpResponse response = get("/profiler/realtime").getResponse();

    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
  }

  @Test
  public void getProfilerMetadata() throws Exception {

    HttpResponse response = get("/profiler/metadata").getResponse();

    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
  }
}
