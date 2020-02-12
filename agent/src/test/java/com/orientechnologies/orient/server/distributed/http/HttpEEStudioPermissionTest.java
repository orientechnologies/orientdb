package com.orientechnologies.orient.server.distributed.http;

import org.apache.http.HttpResponse;
import org.junit.Assert;
import org.junit.Test;

public class HttpEEStudioPermissionTest extends EEBaseServerHttpTest {

  @Test
  public void getPermission() {

    HttpResponse response = get("/permissions/all").getResponse();

    Assert.assertEquals(200, response.getStatusLine().getStatusCode());

    response = get("/permissions  /mine").getResponse();

    Assert.assertEquals(200, response.getStatusLine().getStatusCode());

  }

}
