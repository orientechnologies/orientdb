package com.orientechnologies.distribution.integration.demodb;

import com.orientechnologies.distribution.integration.OIntegrationTestTemplate;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * Created by santo-it on 2017-05-24.
 */
@Test
public class ODemoDbFromDocumentationPolymorphismIT extends OIntegrationTestTemplate {

  @Test
  public void test_Polymorphism_Example_1() throws Exception {

    OResultSet resultSet = db.query(
        "MATCH {class: Customers, as: customer, where: (OrderedId=1)}--{Class: Locations, as: location}  RETURN $pathelements");

    Assert.assertEquals(resultSet.stream().count(), 16);

    resultSet.close();
  }

  // example 2 is handled already in other files

}