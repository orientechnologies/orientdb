package com.orientechnologies.distribution.integration.demodb;

import com.orientechnologies.distribution.integration.OIntegrationTestTemplate;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * Created by santo-it on 2017-08-14.
 */
@Test
public class ODemoDbFromDocumentationTraversesIT extends OIntegrationTestTemplate {

  @Test(priority = 1)
  public void test_Traverses_Example_1() throws Exception {

    OResultSet resultSet = db
        .query("TRAVERSE * FROM (\n" + "  SELECT FROM Profiles WHERE Name='Santo' and Surname='OrientDB'\n" + ") MAXDEPTH 3");

    Assert.assertEquals(resultSet.stream().count(), 85);

    resultSet.close();
  }

  @Test(priority = 2)
  public void test_Traverses_Example_2() throws Exception {

    OResultSet resultSet = db.query("TRAVERSE * FROM (\n" + "  SELECT FROM Countries WHERE Name='Italy'\n" + ") MAXDEPTH 3\n");

    Assert.assertEquals(resultSet.stream().count(), 135);

    resultSet.close();
  }
}