package com.orientechnologies.distribution.integration.demodb;

import com.orientechnologies.distribution.integration.OIntegrationTestTemplate;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Created by santo-it on 24/05/2017.
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class ODemoDbFromDocumentationPolymorphismIT extends OIntegrationTestTemplate {

  @Test
  public void test_Polymorphism_Example_1() throws Exception {

    OResultSet resultSet = db.query(
        "MATCH {class: Customers, as: customer, where: (OrderedId=1)}--{Class: Locations, as: location}  RETURN $pathelements"
    );

    assertThat(resultSet)
        .hasSize(16);

    resultSet.close();
    db.close();

  }

  @Test
  public void test_Polymorphism_Example_2() throws Exception {

    OResultSet resultSet = db.query("SELECT \n"
        + "  @rid as Service_RID,\n"
        + "  Name as Service_Name,\n"
        + "  Type as Service_Type,\n"
        + "  out('HasReview').size() AS ReviewNumbers \n"
        + "FROM `Services` \n"
        + "ORDER BY ReviewNumbers DESC \n"
        + "LIMIT 3");

    assertThat(resultSet)
        .hasSize(3);

    resultSet.close();
    db.close();

  }
}