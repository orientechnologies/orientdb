package com.orientechnologies.distribution.integration.demodb;

import static org.assertj.core.api.Assertions.assertThat;

import com.orientechnologies.distribution.integration.OIntegrationTestTemplate;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

/** Created by santo-it on 2017-05-24. */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class ODemoDbFromDocumentationPolymorphismIT extends OIntegrationTestTemplate {

  @Test
  public void test_Polymorphism_Example_1() throws Exception {

    OResultSet resultSet =
        db.query(
            "MATCH {class: Customers, as: customer, where: (OrderedId=1)}--{Class: Locations, as: location}  RETURN $pathelements");

    assertThat(resultSet).hasSize(16);

    resultSet.close();
    db.close();
  }

  // example 2 is handled already in other files

}
