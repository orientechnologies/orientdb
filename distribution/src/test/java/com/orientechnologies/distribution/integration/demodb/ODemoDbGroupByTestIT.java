package com.orientechnologies.distribution.integration.demodb;

import com.orientechnologies.distribution.integration.OIntegrationTestTemplate;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * Tests for issue #7661
 */
@Test
public class ODemoDbGroupByTestIT extends OIntegrationTestTemplate {

  @Test(priority = 1)
  public void testGroupBy1() throws Exception {
    OResultSet resultSet = db.query("SELECT count(*) FROM Orders GROUP BY OrderDate.format('yyyy')");

    Assert.assertEquals(resultSet.stream().count(), 7);
    resultSet.close();
  }

  @Test(priority = 2)
  public void testGroupBy2() throws Exception {

    OResultSet resultSet = db.query("SELECT count(*), OrderDate.format('yyyy') as year FROM Orders GROUP BY year");

    Assert.assertEquals(resultSet.stream().count(), 7);
    resultSet.close();
  }

  @Test(priority = 3)
  public void testGroupBy3() throws Exception {

    OResultSet resultSet = db.query("SELECT count(*), OrderDate.format('yyyy') FROM Orders GROUP BY OrderDate.format('yyyy')");

    Assert.assertEquals(resultSet.stream().count(), 7);
    resultSet.close();
  }

  @Test(priority = 4)
  public void testGroupBy4() throws Exception {

    OResultSet resultSet = db
        .query("SELECT count(*), OrderDate.format('yyyy') as year FROM Orders GROUP BY OrderDate.format('yyyy')");

    Assert.assertEquals(resultSet.stream().count(), 7);
    resultSet.close();
  }
}