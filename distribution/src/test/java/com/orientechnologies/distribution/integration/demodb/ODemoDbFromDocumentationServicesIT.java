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
public class ODemoDbFromDocumentationServicesIT extends OIntegrationTestTemplate {

  @Test(priority = 1)
  public void test_Services_Example_1() throws Exception {

    OResultSet resultSet = db.query(
        "MATCH {class: Customers, as: customer, where: (OrderedId=1)}--{Class: Services, as: service}\n" + "RETURN $pathelements");

    final List<OResult> results = resultSet.stream().collect(Collectors.toList());
    Assert.assertEquals(results.size(), 8);

    resultSet.close();
  }

  @Test(priority = 2)
  public void test_Services_Example_2() throws Exception {

    OResultSet resultSet = db.query("SELECT \n" + "  Name, Type, in(\"HasStayed\").size() AS NumberOfBookings \n" + "FROM Hotels \n"
        + "ORDER BY NumberOfBookings DESC \n" + "LIMIT 3");

    final List<OResult> results = resultSet.stream().collect(Collectors.toList());
    Assert.assertEquals(results.size(), 3);

    final OResult result = results.iterator().next();

    Assert.assertEquals(result.getProperty("Name"), "Hotel Cavallino d'Oro");
    Assert.assertEquals(result.getProperty("Type"), "hotel");
    Assert.assertEquals(result.<Integer>getProperty("NumberOfBookings"), Integer.valueOf(7));

    resultSet.close();
  }

  @Test(priority = 3)
  public void test_Services_Example_3() throws Exception {

    OResultSet resultSet = db.query("SELECT \n" + "  Name, Type, out(\"HasReview\").size() AS ReviewNumbers \n" + "FROM `Hotels` \n"
        + "ORDER BY ReviewNumbers DESC \n" + "LIMIT 3");

    final List<OResult> results = resultSet.stream().collect(Collectors.toList());
    Assert.assertEquals(results.size(), 3);

    final OResult result = results.iterator().next();

    Assert.assertEquals(result.getProperty("Name"), "Hotel Felicyta");
    Assert.assertEquals(result.getProperty("Type"), "hotel");
    Assert.assertEquals(result.<Integer>getProperty("ReviewNumbers"), Integer.valueOf(5));

    resultSet.close();
  }

  @Test(priority = 4)
  public void test_Services_Example_4() throws Exception {

    OResultSet resultSet = db.query("SELECT \n" + "  Name, \n" + "  count(*) as CountryCount \n" + "FROM (\n" + "  SELECT \n"
        + "    expand(out('IsFromCountry')) AS countries \n" + "  FROM (\n" + "    SELECT \n"
        + "      expand(in(\"HasEaten\")) AS customers \n" + "    FROM Restaurants \n" + "    WHERE Id='26' \n"
        + "    UNWIND customers) \n" + "  UNWIND countries) \n" + "GROUP BY Name \n" + "ORDER BY CountryCount DESC \n" + "LIMIT 3");

    final List<OResult> results = resultSet.stream().collect(Collectors.toList());
    Assert.assertEquals(results.size(), 3);

    final OResult result = results.iterator().next();

    Assert.assertEquals(result.getProperty("Name"), "Croatia");
    Assert.assertEquals(result.<Long>getProperty("CountryCount"), Long.valueOf(1));

    resultSet.close();
  }
}
