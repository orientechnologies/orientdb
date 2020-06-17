package com.orientechnologies.orient.test.server.network.http;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import org.apache.http.HttpEntity;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.junit.Test;

public class HttpDisabledTokenTest extends BaseHttpDatabaseTest {

  protected String getServerCfg() {
    return "/com/orientechnologies/orient/server/network/orientdb-server-config-httponly-notoken.xml";
  };

  @Test
  public void testTokenRequest() throws ClientProtocolException, IOException {
    HttpPost request = new HttpPost(getBaseURL() + "/token/" + getDatabaseName());
    request.setEntity(new StringEntity("grant_type=password&username=admin&password=admin"));
    final CloseableHttpClient httpClient = HttpClients.createDefault();
    CloseableHttpResponse response = httpClient.execute(request);
    assertEquals(response.getStatusLine().getStatusCode(), 400);
    HttpEntity entity = response.getEntity();
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    entity.writeTo(out);
    assertTrue(out.toString().toString().contains("unsupported_grant_type"));
  }

  @Override
  protected String getDatabaseName() {
    return "token_test";
  }
}
