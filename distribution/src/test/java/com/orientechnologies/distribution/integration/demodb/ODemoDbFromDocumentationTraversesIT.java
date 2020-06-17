package com.orientechnologies.distribution.integration.demodb;

import static org.assertj.core.api.Assertions.assertThat;

import com.orientechnologies.distribution.integration.OIntegrationTestTemplate;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

/** Created by santo-it on 2017-08-14. */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class ODemoDbFromDocumentationTraversesIT extends OIntegrationTestTemplate {

  @Test
  public void test_Traverses_Example_1() throws Exception {

    OResultSet resultSet =
        db.query(
            "TRAVERSE * FROM (\n"
                + "  SELECT FROM Profiles WHERE Name='Santo' and Surname='OrientDB'\n"
                + ") MAXDEPTH 3");

    assertThat(resultSet).hasSize(85);

    resultSet.close();
    db.close();
  }

  @Test
  public void test_Traverses_Example_2() throws Exception {

    OResultSet resultSet =
        db.query(
            "TRAVERSE * FROM (\n"
                + "  SELECT FROM Countries WHERE Name='Italy'\n"
                + ") MAXDEPTH 3\n");

    assertThat(resultSet).hasSize(135);

    resultSet.close();
    db.close();
  }
}
