package com.orientechnologies.distribution.integration.demodb;

import com.orientechnologies.distribution.integration.OIntegrationTestTemplate;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * Created by santo-it on 2017-08-14.
 */
@Test
public class ODemoDbFromDocumentationShortestPathsIT extends OIntegrationTestTemplate {

  @Test(priority = 1)
  public void test_ShortestPaths_Example_1() throws Exception {

    OResultSet resultSet = db.query("SELECT expand(path) FROM (\n" + "  SELECT shortestPath($from, $to) AS path \n" + "  LET \n"
        + "    $from = (SELECT FROM Profiles WHERE Name='Santo' and Surname='OrientDB'), \n"
        + "    $to = (SELECT FROM Countries WHERE Name='United States') \n" + "  UNWIND path\n" + ")");

    Assert.assertEquals(resultSet.stream().count(), 4);

    resultSet.close();
  }

  @Test(priority = 2)
  public void test_ShortestPaths_Example_2() throws Exception {

    OResultSet resultSet = db.query("SELECT expand(path) FROM (\n" + "  SELECT shortestPath($from, $to) AS path \n" + "  LET \n"
        + "    $from = (SELECT FROM Profiles WHERE Name='Santo' and Surname='OrientDB'), \n"
        + "    $to = (SELECT FROM Restaurants WHERE Name='Malga Granezza') \n" + "  UNWIND path\n" + ")");

    Assert.assertEquals(resultSet.stream().count(), 4);

    resultSet.close();
  }
}