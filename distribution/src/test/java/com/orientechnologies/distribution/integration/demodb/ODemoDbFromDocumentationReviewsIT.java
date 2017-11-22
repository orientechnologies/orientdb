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
public class ODemoDbFromDocumentationReviewsIT extends OIntegrationTestTemplate {

  @Test(priority = 1)
  public void test_Reviews_Example_1() throws Exception {

    OResultSet resultSet = db
        .query("SELECT \n" + "  Stars, count(*) as Count \n" + "FROM HasReview \n" + "GROUP BY Stars \n" + "ORDER BY Count DESC");

    final List<OResult> results = resultSet.stream().collect(Collectors.toList());
    Assert.assertEquals(results.size(), 5);

    final OResult result = results.iterator().next();

    Assert.assertEquals(result.<Integer>getProperty("Stars"), Integer.valueOf(2));
    Assert.assertEquals(result.<Long>getProperty("Count"), Long.valueOf(272));

    resultSet.close();
  }

  @Test(priority = 2)
  public void test_Reviews_Example_2() throws Exception {

    OResultSet resultSet = db
        .query("MATCH {class: Services, as: s}-HasReview->{class: Reviews, as: r} \n" + "RETURN $pathelements");

    final List<OResult> results = resultSet.stream().collect(Collectors.toList());
    Assert.assertEquals(results.size(), 2546);

    resultSet.close();
  }

  @Test(priority = 3)
  public void test_Reviews_Example_3() throws Exception {
    OResultSet resultSet = db.query(
        "MATCH {class: Services, as: s}-HasReview->{class: Reviews, as: r}<-MadeReview-{class: Customers, as: c} \n"
            + "RETURN $pathelements");

    final List<OResult> results = resultSet.stream().collect(Collectors.toList());
    Assert.assertEquals(results.size(), 3819);

    resultSet.close();
  }

  @Test(priority = 4)
  public void test_Reviews_Example_4() throws Exception {

    OResultSet resultSet = db.query(
        "SELECT \n" + "  @rid as Service_RID,\n" + "  Name as Service_Name,\n" + "  Type as Service_Type,\n"
            + "  out(\"HasReview\").size() AS ReviewNumbers \n" + "FROM `Services` \n" + "ORDER BY ReviewNumbers DESC");

    final List<OResult> results = resultSet.stream().collect(Collectors.toList());
    Assert.assertEquals(results.size(), 3105);

    final OResult result = results.iterator().next();

    Assert.assertEquals(result.getProperty("Service_Name"), "Hotel Felicyta");
    Assert.assertEquals(result.getProperty("Service_Type"), "hotel");
    Assert.assertEquals(result.<Integer>getProperty("ReviewNumbers"), Integer.valueOf(5));

    resultSet.close();
  }

  @Test(priority = 5)
  public void test_Reviews_Example_5() throws Exception {

    OResultSet resultSet = db.query(
        "SELECT\n" + "  @rid as Restaurant_RID,\n" + "  Name as Restaurants_Name,\n" + "  Type as Restaurants_Type,\n"
            + "  out(\"HasReview\").size() AS ReviewNumbers \n" + "FROM `Restaurants` \n" + "ORDER BY ReviewNumbers DESC \n"
            + "LIMIT 3");

    final List<OResult> results = resultSet.stream().collect(Collectors.toList());
    Assert.assertEquals(results.size(), 3);

    final OResult result = results.iterator().next();

    Assert.assertEquals(result.getProperty("Restaurants_Name"), "Pizzeria Il Pirata");
    Assert.assertEquals(result.getProperty("Restaurants_Type"), "restaurant");
    Assert.assertEquals(result.<Integer>getProperty("ReviewNumbers"), Integer.valueOf(4));

    resultSet.close();
  }

  @Test(priority = 6)
  public void test_Reviews_Example_5_bis() throws Exception {

    OResultSet resultSet = db.query(
        "SELECT \n" + "  @rid as Service_RID,\n" + "  Name as Service_Name,\n" + "  Type as Service_Type,\n"
            + "  out(\"HasReview\").size() AS ReviewNumbers \n" + "FROM `Services` \n" + "ORDER BY ReviewNumbers DESC \n"
            + "LIMIT 3");

    final List<OResult> results = resultSet.stream().collect(Collectors.toList());
    Assert.assertEquals(results.size(), 3);

    final OResult result = results.iterator().next();

    Assert.assertEquals(result.getProperty("Service_Name"), "Hotel Felicyta");
    Assert.assertEquals(result.getProperty("Service_Type"), "hotel");
    Assert.assertEquals(result.<Integer>getProperty("ReviewNumbers"), Integer.valueOf(5));

    resultSet.close();
  }

  // example 6 is handled already in other files

}
