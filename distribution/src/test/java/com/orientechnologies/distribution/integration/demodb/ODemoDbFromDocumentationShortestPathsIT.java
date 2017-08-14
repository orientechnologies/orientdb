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
public class ODemoDbFromDocumentationShortestPathsIT extends OIntegrationTestTemplate {

  @Test
  public void test_ShortestPaths_Example_1() throws Exception {

    OResultSet resultSet = db.query("SELECT expand(path) FROM ("
        + "  SELECT shortestPath($from, $to) AS path"
        + "  LET"
        + "    $from = (SELECT FROM Profiles WHERE Name='Santo' and Surname='OrientDB'),"
        + "    $to = (SELECT FROM Countries WHERE Name='United States')"
        + "  UNWIND path" + ")");

    assertThat(resultSet)
        .hasSize(4);

    resultSet.close();
    db.close();
  }

  @Test
  public void test_ShortestPaths_Example_2() throws Exception {

    OResultSet resultSet = db.query("SELECT expand(path) FROM ("
        + "  SELECT shortestPath($from, $to) AS path"
        + "  LET"
        + "    $from = (SELECT FROM Profiles WHERE Name='Santo' and Surname='OrientDB'),"
        + "    $to = (SELECT FROM Restaurants WHERE Name='Malga Granezza')"
        + "  UNWIND path"
        + ")");

    assertThat(resultSet)
        .hasSize(4);

    resultSet.close();
    db.close();
  }

}