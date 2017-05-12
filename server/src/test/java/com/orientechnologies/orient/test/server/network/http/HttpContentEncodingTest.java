package com.orientechnologies.orient.test.server.network.http;

import com.orientechnologies.orient.server.network.protocol.http.OHttpUtils;
import org.apache.http.Header;
import org.apache.http.HttpResponse;
import org.apache.http.message.BasicHeader;
import org.apache.http.util.EntityUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

/**
 * Test HTTP "query" command with 'X-Requested-With': 'XMLHttpRequest' header.
 *
 * @author Sergej Kunz (info@bulktrade.org)
 */

public class HttpContentEncodingTest extends BaseHttpDatabaseTest {
  protected void onAfterDatabaseCreated() throws Exception {
    setUserPassword("123456");
  }

  @Test
  public void sendGZIPRequest() throws IOException {
    Header[] headers = {
      new BasicHeader("Accept-Encoding", OHttpUtils.CONTENT_ACCEPT_GZIP_ENCODED)
    };

    HttpResponse response = get("class/" + getDatabaseName() + "/OUser", headers).getResponse();

    String responseContent = EntityUtils.toString(response.getEntity());

    Assert.assertNotEquals(responseContent, "");
    Assert.assertNotEquals(responseContent, null);
  }

  @Test
  public void sendDeflateRequest() throws IOException {
    Header[] headers = {
        new BasicHeader("Accept-Encoding", OHttpUtils.CONTENT_ACCEPT_DEFLATE_ENCODED)
    };

    HttpResponse response = get("class/" + getDatabaseName() + "/OUser", headers).getResponse();

    String responseContent = EntityUtils.toString(response.getEntity());

    Assert.assertNotEquals(responseContent, "");
    Assert.assertNotEquals(responseContent, null);
  }

  @Override
  public String getDatabaseName() {
    return "httpclassxrequested";
  }
}
