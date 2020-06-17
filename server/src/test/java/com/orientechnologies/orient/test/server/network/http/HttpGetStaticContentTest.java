package com.orientechnologies.orient.test.server.network.http;

import com.orientechnologies.common.io.OIOUtils;
import com.orientechnologies.common.util.OCallable;
import com.orientechnologies.orient.server.network.OServerNetworkListener;
import com.orientechnologies.orient.server.network.protocol.http.ONetworkProtocolHttpAbstract;
import com.orientechnologies.orient.server.network.protocol.http.command.get.OServerCommandGetStaticContent;
import java.io.BufferedInputStream;
import java.net.URL;
import org.apache.http.HttpResponse;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Tests HTTP "static content" command.
 *
 * @author Enrico Risa
 */
public class HttpGetStaticContentTest extends BaseHttpTest {

  @Before
  public void setupFolder() {
    registerFakeVirtualFolder();
  }

  public void registerFakeVirtualFolder() {
    OCallable oCallable =
        new OCallable<Object, String>() {
          @Override
          public Object call(final String iArgument) {
            ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
            final URL url = classLoader.getResource(iArgument);

            if (url != null) {
              final OServerCommandGetStaticContent.OStaticContent content =
                  new OServerCommandGetStaticContent.OStaticContent();
              content.is = new BufferedInputStream(classLoader.getResourceAsStream(iArgument));
              content.contentSize = -1;
              content.type = OServerCommandGetStaticContent.getContentType(url.getFile());
              return content;
            }
            return null;
          }
        };
    final OServerNetworkListener httpListener =
        getServer().getListenerByProtocol(ONetworkProtocolHttpAbstract.class);
    final OServerCommandGetStaticContent command =
        (OServerCommandGetStaticContent)
            httpListener.getCommand(OServerCommandGetStaticContent.class);
    command.registerVirtualFolder("fake", oCallable);
  }

  @Test
  public void testIndexHTML() throws Exception {
    HttpResponse response = get("fake/index.htm").getResponse();
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());

    String expected =
        OIOUtils.readStreamAsString(
            this.getClass().getClassLoader().getResourceAsStream("index.htm"));
    String actual = OIOUtils.readStreamAsString(response.getEntity().getContent());
    Assert.assertEquals(expected, actual);
  }

  @Override
  public String getDatabaseName() {
    return "httpdb";
  }

  @Before
  public void startServer() throws Exception {
    super.startServer();
  }

  @After
  public void stopServer() throws Exception {
    super.stopServer();
  }
}
