package com.orientechnologies.distribution.integration.demodb;

import com.orientechnologies.distribution.integration.OIntegrationTestTemplate;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Created by santo-it on 14/08/2017.
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class ODemoDbFromDocumentationTraversesIT extends OIntegrationTestTemplate {

  @Test
  public void test_Traverses_Example_1() throws Exception {

    OResultSet resultSet = db.query("TRAVERSE * FROM ("
                                        + "SELECT FROM Profiles WHERE Name='Santo' and Surname='OrientDB'"
                                        + ") MAXDEPTH 3");

    assertThat(resultSet)
        .hasSize(85);

    resultSet.close();
    db.close();
  }

  @Test
  public void test_Traverses_Example_2() throws Exception {

    OResultSet resultSet = db.query("TRAVERSE * FROM ("
        + "SELECT FROM Countries WHERE Name='Italy'"
        + ") MAXDEPTH 3");

    assertThat(resultSet)
        .hasSize(136);

    resultSet.close();
    db.close();
  }

}