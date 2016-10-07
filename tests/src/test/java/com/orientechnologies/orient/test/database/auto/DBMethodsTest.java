package com.orientechnologies.orient.test.database.auto;

import org.testng.Assert;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

/**
 * @author Andrey Lomakin (a.lomakin-at-orientdb.com)
 * @since 9/15/14
 */
@Test
public class DBMethodsTest extends DocumentDBBaseTest {
  @Parameters(value = "url")
  public DBMethodsTest(@Optional String url) {
    super(url);
  }

  public void testAddCluster() {
    database.addCluster("addClusterTest");

    Assert.assertTrue(database.existsCluster("addClusterTest"));
    Assert.assertTrue(database.existsCluster("addclUstertESt"));
  }
}
