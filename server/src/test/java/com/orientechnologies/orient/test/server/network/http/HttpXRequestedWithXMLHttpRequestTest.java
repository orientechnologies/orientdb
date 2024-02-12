package com.orientechnologies.orient.test.server.network.http;

import java.io.IOException;
import org.apache.hc.core5.http.Header;
import org.apache.hc.core5.http.message.BasicHeader;
import org.junit.Assert;
import org.junit.Test;

/**
 * @author Enrico Risa
 */
public class HttpXRequestedWithXMLHttpRequestTest extends BaseHttpDatabaseTest {
  protected void onAfterDatabaseCreated() throws Exception {
    setUserPassword("123456");
  }

  @Test
  public void sendXMLHttpRequest() throws IOException {
    Header[] headers = {new BasicHeader("X-Requested-With", "XMLHttpRequest")};

    var response = get("class/" + getDatabaseName() + "/OUser", headers).getResponse();

    Assert.assertEquals(response.getCode(), 401);
    Assert.assertEquals(response.containsHeader("WWW-Authenticate"), false);
  }

  @Test
  public void sendHttpRequest() throws IOException {
    var response = get("class/" + getDatabaseName() + "/OUser").getResponse();

    Assert.assertEquals(response.getCode(), 401);
    Assert.assertEquals(response.containsHeader("WWW-Authenticate"), true);
  }

  @Override
  public String getDatabaseName() {
    return "httpclassxrequested";
  }
}
