package com.orientechnologies.orient.server.network.http;

import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * Tests HTTP "cluster" command.
 * 
 * @author Luca Garulli (l.garulli--at-orientechnologies.com)
 */
@Test
public class HttpClusterTest extends BaseHttpDatabaseTest {
  @Test
  public void testExistentClass() throws Exception {
    Assert.assertEquals(get("cluster/" + getDatabaseName() + "/OUser").getResponse().getStatusLine().getStatusCode(), 200);
  }

  @Test
  public void testNonExistentClass() throws Exception {
    Assert.assertEquals(get("cluster/" + getDatabaseName() + "/NonExistentCLass").getResponse().getStatusLine().getStatusCode(),
        404);
  }

  @Override
  public String getDatabaseName() {
    return "httpcluster";
  }

}
