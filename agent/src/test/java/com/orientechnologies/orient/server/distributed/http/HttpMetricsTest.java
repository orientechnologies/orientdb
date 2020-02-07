package com.orientechnologies.orient.server.distributed.http;

import com.orientechnologies.orient.core.record.impl.ODocument;
import org.apache.http.HttpResponse;
import org.apache.http.util.EntityUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.Map;

public class HttpMetricsTest extends EEBaseHttpTest {

  @Test
  public void getMetrics() throws Exception {

    HttpResponse response = get("/metrics").getResponse();

    Assert.assertEquals(200, response.getStatusLine().getStatusCode());

    String body = EntityUtils.toString(getResponse().getEntity());

    ODocument entries = new ODocument().fromJSON(body);

    Map<String, Object> clusterStats = entries.getProperty("clusterStats");

    Assert.assertEquals(3, clusterStats.size());
    Assert.assertTrue(entries.containsField("databasesStatus"));

  }
}
