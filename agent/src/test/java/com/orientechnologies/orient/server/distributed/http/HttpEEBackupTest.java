package com.orientechnologies.orient.server.distributed.http;

import com.orientechnologies.orient.core.record.impl.ODocument;
import org.apache.http.HttpResponse;
import org.junit.Assert;
import org.junit.Test;

public class HttpEEBackupTest extends EEBaseServerHttpTest {

  @Test
  public void getBackupManager() throws Exception {

    HttpResponse response = get("/backupManager").getResponse();

    Assert.assertEquals(200, response.getStatusLine().getStatusCode());

  }

  @Test
  public void postPutDelBackup() throws Exception {

    ODocument backup = new ODocument().fromJSON(Thread.currentThread().getContextClassLoader().getResourceAsStream("backup.json"));

    backup.field("dbName", name.getMethodName());

    HttpResponse response = post("/backupManager").payload(backup.toJSON(), CONTENT.JSON).getResponse();

    Assert.assertEquals(200, response.getStatusLine().getStatusCode());

    ODocument saved = new ODocument().fromJSON(response.getEntity().getContent());

    response = put("/backupManager/" + saved.field("uuid")).payload(saved.toJSON(), CONTENT.JSON).getResponse();

    Assert.assertEquals(200, response.getStatusLine().getStatusCode());

    response = delete("/backupManager/" + saved.field("uuid")).getResponse();

    Assert.assertEquals(204, response.getStatusLine().getStatusCode());

  }
}
