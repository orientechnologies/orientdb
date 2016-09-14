package com.orientechnologies.orient.test.server.network.http;

import org.apache.http.Header;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.message.BasicHeader;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

/**
 * Test HTTP "query" command with 'X-Requested-With': 'XMLHttpRequest' header.
 *
 * @author Sergej Kunz (info@bulktrade.org)
 */

public class HttpXRequestedWithXMLHttpRequestTest extends BaseHttpDatabaseTest {
  protected void onAfterDatabaseCreated() throws Exception {
    setUserPassword("123456");
  }

  @Test
  public void sendXMLHttpRequest() throws IOException {
    Header[] headers = {
      new BasicHeader("X-Requested-With", "XMLHttpRequest")
    };

    HttpResponse response = get("class/" + getDatabaseName() + "/OUser", headers).getResponse();

    Assert.assertEquals(response.getStatusLine().getStatusCode(), 401);
    Assert.assertEquals(response.containsHeader("WWW-Authenticate"), false);
  }

  @Test
  public void sendHttpRequest() throws IOException {
    HttpResponse response = get("class/" + getDatabaseName() + "/OUser").getResponse();

    Assert.assertEquals(response.getStatusLine().getStatusCode(), 401);
    Assert.assertEquals(response.containsHeader("WWW-Authenticate"), true);
  }

  @Override
  public String getDatabaseName() {
    return "httpclassxrequested";
  }
}
