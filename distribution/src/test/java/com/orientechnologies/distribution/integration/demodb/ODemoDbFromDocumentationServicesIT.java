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
public class ODemoDbFromDocumentationServicesIT extends OIntegrationTestTemplate {

  @Test
  public void test_Services_Example_1() throws Exception {

    OResultSet resultSet =
        db.query(
            "MATCH {class: Customers, as: customer, where: (OrderedId=1)}--{Class: Services, as: service}\n"
                + "RETURN $pathelements");

    final List<OResult> results = resultSet.stream().collect(Collectors.toList());
    assertThat(results).hasSize(8);

    resultSet.close();
    db.close();
  }

  @Test
  public void test_Services_Example_2() throws Exception {

    OResultSet resultSet =
        db.query(
            "SELECT \n"
                + "  Name, Type, in(\"HasStayed\").size() AS NumberOfBookings \n"
                + "FROM Hotels \n"
                + "ORDER BY NumberOfBookings DESC \n"
                + "LIMIT 3");

    final List<OResult> results = resultSet.stream().collect(Collectors.toList());
    assertThat(results).hasSize(3);

    final OResult result = results.iterator().next();

    assertThat(result.<String>getProperty("Name")).isEqualTo("Hotel Cavallino d'Oro");
    assertThat(result.<String>getProperty("Type")).isEqualTo("hotel");
    assertThat(result.<Integer>getProperty("NumberOfBookings")).isEqualTo(7);

    resultSet.close();
    db.close();
  }

  @Test
  public void test_Services_Example_3() throws Exception {

    OResultSet resultSet =
        db.query(
            "SELECT \n"
                + "  Name, Type, out(\"HasReview\").size() AS ReviewNumbers \n"
                + "FROM `Hotels` \n"
                + "ORDER BY ReviewNumbers DESC \n"
                + "LIMIT 3");

    final List<OResult> results = resultSet.stream().collect(Collectors.toList());
    assertThat(results).hasSize(3);

    final OResult result = results.iterator().next();

    assertThat(result.<String>getProperty("Name")).isEqualTo("Hotel Felicyta");
    assertThat(result.<String>getProperty("Type")).isEqualTo("hotel");
    assertThat(result.<Integer>getProperty("ReviewNumbers")).isEqualTo(5);

    resultSet.close();
    db.close();
  }

  @Test
  public void test_Services_Example_4() throws Exception {

    OResultSet resultSet =
        db.query(
            "SELECT \n"
                + "  Name, \n"
                + "  count(*) as CountryCount \n"
                + "FROM (\n"
                + "  SELECT \n"
                + "    expand(out('IsFromCountry')) AS countries \n"
                + "  FROM (\n"
                + "    SELECT \n"
                + "      expand(in(\"HasEaten\")) AS customers \n"
                + "    FROM Restaurants \n"
                + "    WHERE Id='26' \n"
                + "    UNWIND customers) \n"
                + "  UNWIND countries) \n"
                + "GROUP BY Name \n"
                + "ORDER BY CountryCount DESC \n"
                + "LIMIT 3");

    final List<OResult> results = resultSet.stream().collect(Collectors.toList());
    assertThat(results).hasSize(3);

    final OResult result = results.iterator().next();

    assertThat(result.<String>getProperty("Name")).isEqualTo("Croatia");
    assertThat(result.<Long>getProperty("CountryCount")).isEqualTo(1);

    resultSet.close();
    db.close();
  }
}
