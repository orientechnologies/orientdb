package com.orientechnologies.distribution.integration.demodb;

import com.orientechnologies.distribution.integration.OIntegrationTestTemplate;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.List;
import java.util.stream.Collectors;
/**
 * Created by santo-it on 2017-08-28.
 */
@Test
public class ODemoDbFromDocumentationLocationsIT extends OIntegrationTestTemplate {
  @Test(priority = 1)
  public void test_Locations_Example_1() throws Exception {

    OResultSet resultSet = db.query(
        "MATCH {Class: Profiles, as: profile, where: (Name='Santo' AND Surname='OrientDB')}<-HasProfile-{Class: Customers, as: customer}-HasVisited->{class: Locations, as: location} \n"
            + "RETURN $pathelements");

    final List<OResult> results = resultSet.stream().collect(Collectors.toList());
    Assert.assertEquals(results.size(), 12);

    resultSet.close();
  }

  // examples 2 and 3 are handled already in other files

  @Test(priority = 2)
  public void test_Locations_Example_4() throws Exception {

    OResultSet resultSet = db.query(
        "MATCH {Class: Locations, as: location}<-HasVisited-{class: Customers, as: customer, where: (OrderedId=2)}\n"
            + "RETURN $pathelements");

    final List<OResult> results = resultSet.stream().collect(Collectors.toList());
    Assert.assertEquals(results.size(), 46);

    resultSet.close();
  }

  @Test(priority = 3)
  public void test_Locations_Example_5() throws Exception {

    OResultSet resultSet = db.query(
        "MATCH {Class: Profiles, as: profile, where: (Name='Santo' and Surname='OrientDB')}-HasFriend->{Class: Profiles, as: friend}<-HasProfile-{Class: Customers, as: customer}-HasVisited->{Class: Locations, as: location} \n"
            + "RETURN $pathelements");

    final List<OResult> results = resultSet.stream().collect(Collectors.toList());
    Assert.assertEquals(results.size(), 124);

    resultSet.close();
  }

}
