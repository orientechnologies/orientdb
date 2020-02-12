package com.orientechnologies.orient.server.distributed.http;

import org.apache.http.HttpResponse;
import org.junit.Assert;
import org.junit.Test;

public class HttpEEBackupTest extends EEBaseServerHttpTest {

  @Test
  public void getBackupManager() throws Exception {

    HttpResponse response = get("/backupManager").getResponse();

    Assert.assertEquals(200, response.getStatusLine().getStatusCode());

  }
}
