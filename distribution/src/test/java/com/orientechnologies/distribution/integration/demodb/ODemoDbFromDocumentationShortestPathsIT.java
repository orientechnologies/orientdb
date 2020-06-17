package com.orientechnologies.distribution.integration.demodb;

import static org.assertj.core.api.Assertions.assertThat;

import com.orientechnologies.distribution.integration.OIntegrationTestTemplate;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

/** Created by santo-it on 2017-08-14. */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class ODemoDbFromDocumentationShortestPathsIT extends OIntegrationTestTemplate {

  @Test
  public void test_ShortestPaths_Example_1() throws Exception {

    OResultSet resultSet =
        db.query(
            "SELECT expand(path) FROM (\n"
                + "  SELECT shortestPath($from, $to) AS path \n"
                + "  LET \n"
                + "    $from = (SELECT FROM Profiles WHERE Name='Santo' and Surname='OrientDB'), \n"
                + "    $to = (SELECT FROM Countries WHERE Name='United States') \n"
                + "  UNWIND path\n"
                + ")");

    assertThat(resultSet).hasSize(4);

    resultSet.close();
    db.close();
  }

  @Test
  public void test_ShortestPaths_Example_2() throws Exception {

    OResultSet resultSet =
        db.query(
            "SELECT expand(path) FROM (\n"
                + "  SELECT shortestPath($from, $to) AS path \n"
                + "  LET \n"
                + "    $from = (SELECT FROM Profiles WHERE Name='Santo' and Surname='OrientDB'), \n"
                + "    $to = (SELECT FROM Restaurants WHERE Name='Malga Granezza') \n"
                + "  UNWIND path\n"
                + ")");

    assertThat(resultSet).hasSize(4);

    resultSet.close();
    db.close();
  }
}
