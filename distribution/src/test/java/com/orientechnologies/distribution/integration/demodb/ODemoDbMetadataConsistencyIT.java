package com.orientechnologies.distribution.integration.demodb;

import com.orientechnologies.distribution.integration.OIntegrationTestTemplate;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Created by frank on 15/03/2017.
 */
@Test
public class ODemoDbMetadataConsistencyIT extends OIntegrationTestTemplate {

  private static final int vCount           = 7275;
  private static final int locationsCount   = 3541;
  private static final int attractionsCount = 436;
  private static final int archSitesCount   = 55;
  private static final int castlesCount     = 127;
  private static final int monumentsCount   = 137;
  private static final int theatresCount    = 117;
  private static final int ServicesCount    = 3105;
  private static final int hotelsCount      = 1154;
  private static final int restaurantsCount = 1951;
  private static final int profilesCount    = 1000;
  private static final int customersCount   = 400;
  private static final int countriesCount   = 249;
  private static final int ordersCount      = 812;
  private static final int reviewsCount     = 1273;

  private static final int eCount              = 14872;
  private static final int hasCustomerCount    = 812;
  private static final int hasEatenCount       = 2479;
  private static final int hasFriendCount      = 1617;
  private static final int hasProfileCount     = 400;
  private static final int hasReviewCount      = 1273;
  private static final int hasStayedCount      = 1645;
  private static final int hasUsedServiceCount = 4124;
  private static final int hasVisitedCount     = 4973;
  private static final int isFromCountryCount  = 400;
  private static final int madeReviewCount     = 1273;

  @Test
  public void testMetadata() throws Exception {

    OSchema schema = db.getMetadata().getSchema();

    //todo: properties & indices

    // vertices
    Assert.assertNotNull(schema.getClass("V"));
    Assert.assertEquals(schema.getClass("V").getSubclasses().size(), 14);
    Assert.assertEquals(schema.getClass("V").count(), vCount);

    Assert.assertNotNull(schema.getClass("Locations"));
    Assert.assertEquals(schema.getClass("Locations").getSuperClassesNames().get(0), "V");
    Assert.assertEquals(schema.getClass("Locations").getSubclasses().size(), 2);
    Assert.assertEquals(schema.getClass("Locations").count(), locationsCount);

    Assert.assertNotNull(schema.getClass("Attractions"));
    Assert.assertEquals(schema.getClass("Attractions").getSuperClassesNames().get(0), "V");
    Assert.assertEquals(schema.getClass("Attractions").getSuperClassesNames().get(1), "Locations");
    Assert.assertEquals(schema.getClass("Attractions").getSubclasses().size(), 4);
    Assert.assertEquals(schema.getClass("Attractions").count(), attractionsCount);

    Assert.assertNotNull(schema.getClass("ArchaeologicalSites"));
    Assert.assertEquals(schema.getClass("ArchaeologicalSites").getSuperClassesNames().get(0), "V");
    Assert.assertEquals(schema.getClass("ArchaeologicalSites").getSuperClassesNames().get(1), "Attractions");
    Assert.assertEquals(schema.getClass("ArchaeologicalSites").count(), archSitesCount);

    Assert.assertNotNull(schema.getClass("Castles"));
    Assert.assertEquals(schema.getClass("Castles").getSuperClassesNames().get(0), "V");
    Assert.assertEquals(schema.getClass("Castles").getSuperClassesNames().get(1), "Attractions");
    Assert.assertEquals(schema.getClass("Castles").count(), castlesCount);

    Assert.assertNotNull(schema.getClass("Monuments"));
    Assert.assertEquals(schema.getClass("Monuments").getSuperClassesNames().get(0), "V");
    Assert.assertEquals(schema.getClass("Monuments").getSuperClassesNames().get(1), "Attractions");
    Assert.assertEquals(schema.getClass("Monuments").count(), monumentsCount);

    Assert.assertNotNull(schema.getClass("Theatres"));
    Assert.assertEquals(schema.getClass("Theatres").getSuperClassesNames().get(0), "V");
    Assert.assertEquals(schema.getClass("Theatres").getSuperClassesNames().get(1), "Attractions");
    Assert.assertEquals(schema.getClass("Theatres").count(), theatresCount);

    Assert.assertNotNull(schema.getClass("Services"));
    Assert.assertEquals(schema.getClass("Services").getSuperClassesNames().get(0), "V");
    Assert.assertEquals(schema.getClass("Services").getSuperClassesNames().get(1), "Locations");
    Assert.assertEquals(schema.getClass("Services").getSubclasses().size(), 2);
    Assert.assertEquals(schema.getClass("Services").count(), ServicesCount);

    Assert.assertNotNull(schema.getClass("Hotels"));
    Assert.assertEquals(schema.getClass("Hotels").getSuperClassesNames().get(0), "V");
    Assert.assertEquals(schema.getClass("Hotels").count(), hotelsCount);

    Assert.assertNotNull(schema.getClass("Restaurants"));
    Assert.assertEquals(schema.getClass("Restaurants").getSuperClassesNames().get(0), "V");
    Assert.assertEquals(schema.getClass("Restaurants").count(), restaurantsCount);

    Assert.assertNotNull(schema.getClass("Profiles"));
    Assert.assertEquals(schema.getClass("Profiles").getSuperClassesNames().get(0), "V");
    Assert.assertEquals(schema.getClass("Profiles").count(), profilesCount);

    Assert.assertNotNull(schema.getClass("Customers"));
    Assert.assertEquals(schema.getClass("Customers").getSuperClassesNames().get(0), "V");
    Assert.assertEquals(schema.getClass("Customers").count(), customersCount);

    Assert.assertNotNull(schema.getClass("Countries"));
    Assert.assertEquals(schema.getClass("Countries").getSuperClassesNames().get(0), "V");
    Assert.assertEquals(schema.getClass("Countries").count(), countriesCount);

    Assert.assertNotNull(schema.getClass("Orders"));
    Assert.assertEquals(schema.getClass("Orders").getSuperClassesNames().get(0), "V");
    Assert.assertEquals(schema.getClass("Orders").count(), ordersCount);

    Assert.assertNotNull(schema.getClass("Reviews"));
    Assert.assertEquals(schema.getClass("Reviews").getSuperClassesNames().get(0), "V");
    Assert.assertEquals(schema.getClass("Reviews").count(), reviewsCount);
    //

    // edges
    Assert.assertNotNull(schema.getClass("E"));
    Assert.assertEquals(schema.getClass("E").getSubclasses().size(), 10);
    Assert.assertEquals(schema.getClass("E").count(), eCount);

    Assert.assertNotNull(schema.getClass("HasCustomer"));
    Assert.assertEquals(schema.getClass("HasCustomer").getSuperClassesNames().get(0), "E");
    Assert.assertEquals(schema.getClass("HasCustomer").count(), hasCustomerCount);

    Assert.assertNotNull(schema.getClass("HasEaten"));
    Assert.assertEquals(schema.getClass("HasEaten").getSuperClassesNames().get(0), "E");
    Assert.assertEquals(schema.getClass("HasEaten").getSuperClassesNames().get(1), "HasUsedService");
    Assert.assertEquals(schema.getClass("HasEaten").count(), hasEatenCount);

    Assert.assertNotNull(schema.getClass("HasFriend"));
    Assert.assertEquals(schema.getClass("HasFriend").getSuperClassesNames().get(0), "E");
    Assert.assertEquals(schema.getClass("HasFriend").count(), hasFriendCount);

    Assert.assertNotNull(schema.getClass("HasProfile"));
    Assert.assertEquals(schema.getClass("HasProfile").getSuperClassesNames().get(0), "E");
    Assert.assertEquals(schema.getClass("HasProfile").count(), hasProfileCount);

    Assert.assertNotNull(schema.getClass("HasReview"));
    Assert.assertEquals(schema.getClass("HasReview").getSuperClassesNames().get(0), "E");
    Assert.assertEquals(schema.getClass("HasReview").count(), hasReviewCount);

    Assert.assertNotNull(schema.getClass("HasStayed"));
    Assert.assertEquals(schema.getClass("HasStayed").getSuperClassesNames().get(0), "E");
    Assert.assertEquals(schema.getClass("HasStayed").getSuperClassesNames().get(1), "HasUsedService");
    Assert.assertEquals(schema.getClass("HasStayed").count(), hasStayedCount);

    Assert.assertNotNull(schema.getClass("HasUsedService"));
    Assert.assertEquals(schema.getClass("HasUsedService").getSuperClassesNames().get(0), "E");
    Assert.assertEquals(schema.getClass("HasUsedService").getSubclasses().size(), 2);
    Assert.assertEquals(schema.getClass("HasUsedService").count(), hasUsedServiceCount);

    //other way to check inheritance
    List<OResult> results = db.query("SELECT DISTINCT(@class) AS className from `HasUsedService` ORDER BY className ASC").stream()
        .collect(Collectors.toList());
    Assert.assertEquals(results.size(), 2);
    Assert.assertEquals(results.get(0).getProperty("className"), "HasEaten");
    Assert.assertEquals(results.get(1).getProperty("className"), "HasStayed");
    //

    Assert.assertNotNull(schema.getClass("HasVisited"));
    Assert.assertEquals(schema.getClass("HasVisited").getSuperClassesNames().get(0), "E");
    Assert.assertEquals(schema.getClass("HasVisited").count(), hasVisitedCount);

    Assert.assertNotNull(schema.getClass("IsFromCountry"));
    Assert.assertEquals(schema.getClass("IsFromCountry").getSuperClassesNames().get(0), "E");
    Assert.assertEquals(schema.getClass("IsFromCountry").count(), isFromCountryCount);

    Assert.assertNotNull(schema.getClass("MadeReview"));
    Assert.assertEquals(schema.getClass("MadeReview").getSuperClassesNames().get(0), "E");
    Assert.assertEquals(schema.getClass("MadeReview").count(), madeReviewCount);
    //

  }

  @Test
  public void testDataModel() throws Exception {

    // all customers have a country
    OResultSet resultSet = db
        .query("MATCH {class: Customers, as: customer}-IsFromCountry->{class: Countries, as: country} RETURN  customer");
    Assert.assertEquals(resultSet.stream().count(), customersCount);
    resultSet.close();

    // all customers have a profile
    resultSet = db.query("MATCH {class: Customers, as: customer}-HasProfile->{class: Profiles, as: profile} RETURN customer");
    Assert.assertEquals(resultSet.stream().count(), customersCount);
    resultSet.close();

    // all customers have at least 1 order
    resultSet = db.query("MATCH {class: Orders, as: order}-HasCustomer->{class: Customers, as:customer} RETURN order");
    Assert.assertTrue(resultSet.stream().count() > customersCount);
    resultSet.close();

  }

  @Test
  public void testMatchWithConditionInBackTraversal() throws Exception {
    OResultSet resultSet = db.query("MATCH \n"
        + "{class:Profiles, as:profileA} <-HasProfile- {as:customerA} -MadeReview-> {as:reviewA} <-HasReview- {as:restaurant},\n"
        + "{as:profileB, where:($matched.profileA != $currentMatch)} <-HasProfile- {as:customerB} -MadeReview-> {as:reviewB} <-HasReview- {as:restaurant}\n"
        + "return  profileA.Id as idA, profileA.Name, profileA.Surname, profileB.Id as idA, profileB.Name, profileB.Surname\n"
        + "limit 10\n");

    int size = 0;
    while (resultSet.hasNext()) {
      OResult item = resultSet.next();
      Assert.assertNotEquals(item.getProperty("idA"), item.getProperty("idB"));
      size++;
    }

    Assert.assertEquals(size, 10);
    resultSet.close();
  }
}
