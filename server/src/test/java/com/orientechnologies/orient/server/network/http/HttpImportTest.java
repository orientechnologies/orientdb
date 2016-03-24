package com.orientechnologies.orient.server.network.http;

import org.apache.http.HttpResponse;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.internal.util.io.IOUtil;
import sun.misc.IOUtils;

import java.io.IOException;

import static org.junit.Assert.assertEquals;

/**
 * Created by tglman on 16/03/16.
 */
public class HttpImportTest extends BaseHttpDatabaseTest {

  @Test
  public void testImport() throws IOException {

    String content = "{\"records\": [{\"@type\": \"d\", \"@rid\": \"#9:0\",\"@version\": 1,\"@class\": \"V\"}]}";
    post("import/" + getDatabaseName() + "?merge=true").payload(content, CONTENT.TEXT);
    HttpResponse response = getResponse();
    assertEquals(200, response.getStatusLine().getStatusCode());

    System.out.println(IOUtil.readLines(response.getEntity().getContent()));

  }

  @Override
  protected String getDatabaseName() {
    return this.getClass().getSimpleName();
  }


}
