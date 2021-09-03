package com.orientechnologies.distribution.integration.demodb;

import static org.assertj.core.api.Assertions.assertThat;

import com.orientechnologies.distribution.integration.OIntegrationTestTemplate;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import java.util.List;
import java.util.stream.Collectors;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

/** Created by santo-it on 2017-08-28. */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class ODemoDbFromDocumentationLocationsIT extends OIntegrationTestTemplate {

  @Test
  public void test_Locations_Example_1() throws Exception {

    OResultSet resultSet =
        db.query(
            "MATCH {Class: Profiles, as: profile, where: (Name='Santo' AND Surname='OrientDB')}<-HasProfile-{Class: Customers, as: customer}-HasVisited->{class: Locations, as: location} \n"
                + "RETURN $pathelements");

    final List<OResult> results = resultSet.stream().collect(Collectors.toList());
    assertThat(results).hasSize(12);

    resultSet.close();
    db.close();
  }

  // examples 2 and 3 are handled already in other files

  @Test
  public void test_Locations_Example_4() throws Exception {

    OResultSet resultSet =
        db.query(
            "MATCH {Class: Locations, as: location}<-HasVisited-{class: Customers, as: customer, where: (OrderedId=2)}\n"
                + "RETURN $pathelements");

    final List<OResult> results = resultSet.stream().collect(Collectors.toList());
    assertThat(results).hasSize(46);

    resultSet.close();
    db.close();
  }

  @Test
  public void test_Locations_Example_5() throws Exception {

    OResultSet resultSet =
        db.query(
            "MATCH {Class: Profiles, as: profile, where: (Name='Santo' and Surname='OrientDB')}-HasFriend->{Class: Profiles, as: friend}<-HasProfile-{Class: Customers, as: customer}-HasVisited->{Class: Locations, as: location} \n"
                + "RETURN $pathelements");

    final List<OResult> results = resultSet.stream().collect(Collectors.toList());
    assertThat(results).hasSize(124);

    resultSet.close();
    db.close();
  }
}
