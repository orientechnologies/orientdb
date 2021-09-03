package com.orientechnologies.distribution.integration.demodb;

import static org.assertj.core.api.Assertions.assertThat;

import com.orientechnologies.distribution.integration.OIntegrationTestTemplate;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

/** Tests for issue #7661 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class ODemoDbGroupByTestIT extends OIntegrationTestTemplate {

  @Test
  public void testGroupBy1() throws Exception {
    OResultSet resultSet =
        db.query("SELECT count(*) FROM Orders GROUP BY OrderDate.format('yyyy')");

    assertThat(resultSet).hasSize(7);
    resultSet.close();
    db.close();
  }

  @Test
  public void testGroupBy2() throws Exception {

    OResultSet resultSet =
        db.query("SELECT count(*), OrderDate.format('yyyy') as year FROM Orders GROUP BY year");

    assertThat(resultSet).hasSize(7);
    resultSet.close();
    db.close();
  }

  @Test
  public void testGroupBy3() throws Exception {

    OResultSet resultSet =
        db.query(
            "SELECT count(*), OrderDate.format('yyyy') FROM Orders GROUP BY OrderDate.format('yyyy')");

    assertThat(resultSet).hasSize(7);
    resultSet.close();
    db.close();
  }

  @Test
  public void testGroupBy4() throws Exception {

    OResultSet resultSet =
        db.query(
            "SELECT count(*), OrderDate.format('yyyy') as year FROM Orders GROUP BY OrderDate.format('yyyy')");

    assertThat(resultSet).hasSize(7);
    resultSet.close();
    db.close();
  }
}
