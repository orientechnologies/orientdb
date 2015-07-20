package com.orientechnologies.orient.server.network.http;

import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * Tests HTTP "class" command.
 * 
 * @author Luca Garulli (l.garulli--at-orientechnologies.com)
 */
@Test
public class HttpClassTest extends BaseHttpDatabaseTest {
  @Test
  public void testExistentClass() throws Exception {
    Assert.assertEquals(get("class/" + getDatabaseName() + "/OUser").getResponse().getStatusLine().getStatusCode(), 200);
  }

  @Test
    public void testNonExistentClass() throws Exception {
    Assert.assertEquals(get("class/" + getDatabaseName() + "/NonExistentCLass").getResponse().getStatusLine().getStatusCode(), 404);
  }

  @Test
  public void testCreateClass() throws Exception {
    Assert.assertEquals(post("class/" + getDatabaseName() + "/NewClass").getResponse().getStatusLine().getStatusCode(), 201);
  }

  @Test
  public void testDropClass() throws Exception {
    Assert.assertEquals(post("class/" + getDatabaseName() + "/NewClassToDrop").getResponse().getStatusLine().getStatusCode(), 201);
    Assert.assertEquals(delete("class/" + getDatabaseName() + "/NewClassToDrop").getResponse().getStatusLine().getStatusCode(), 204);
  }

  @Override
  public String getDatabaseName() {
    return "httpclass";
  }

}
