package com.orientechnologies.orient.test.server.network.http;

import java.io.IOException;
import org.apache.http.Header;
import org.apache.http.HttpResponse;
import org.apache.http.message.BasicHeader;
import org.junit.Assert;
import org.junit.Test;

/** @author Enrico Risa */
public class HttpXRequestedWithXMLHttpRequestTest extends BaseHttpDatabaseTest {
  protected void onAfterDatabaseCreated() throws Exception {
    setUserPassword("123456");
  }

  @Test
  public void sendXMLHttpRequest() throws IOException {
    Header[] headers = {new BasicHeader("X-Requested-With", "XMLHttpRequest")};

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
