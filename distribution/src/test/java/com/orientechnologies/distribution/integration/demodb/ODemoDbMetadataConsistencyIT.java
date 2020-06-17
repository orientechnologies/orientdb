package com.orientechnologies.distribution.integration.demodb;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;

import com.orientechnologies.distribution.integration.OIntegrationTestTemplate;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import java.util.List;
import java.util.stream.Collectors;
import org.junit.Test;

/** Created by frank on 15/03/2017. */
public class ODemoDbMetadataConsistencyIT extends OIntegrationTestTemplate {

  private int vCount = 7275;
  private int locationsCount = 3541;
  private int attractionsCount = 436;
  private int archSitesCount = 55;
  private int castlesCount = 127;
  private int monumentsCount = 137;
  private int theatresCount = 117;
  private int ServicesCount = 3105;
  private int hotelsCount = 1154;
  private int restaurantsCount = 1951;
  private int profilesCount = 1000;
  private int customersCount = 400;
  private int countriesCount = 249;
  private int ordersCount = 812;
  private int reviewsCount = 1273;

  private int eCount = 14872;
  private int hasCustomerCount = 812;
  private int hasEatenCount = 2479;
  private int hasFriendCount = 1617;
  private int hasProfileCount = 400;
  private int hasReviewCount = 1273;
  private int hasStayedCount = 1645;
  private int hasUsedServiceCount = 4124;
  private int hasVisitedCount = 4973;
  private int isFromCountryCount = 400;
  private int madeReviewCount = 1273;

  @Test
  public void testMetadata() throws Exception {

    OSchema schema = db.getMetadata().getSchema();

    // todo: properties & indices

    // vertices
    assertThat(schema.getClass("V")).isNotNull();
    assertThat(schema.getClass("V").getSubclasses()).hasSize(14);
    assertThat(schema.getClass("V").count()).isEqualTo(vCount);

    assertThat(schema.getClass("Locations")).isNotNull();
    assertEquals("V", schema.getClass("Locations").getSuperClassesNames().get(0));
    assertThat(schema.getClass("Locations").getSubclasses()).hasSize(2);
    assertThat(schema.getClass("Locations").count()).isEqualTo(locationsCount);

    assertThat(schema.getClass("Attractions")).isNotNull();
    assertEquals("V", schema.getClass("Attractions").getSuperClassesNames().get(0));
    assertEquals("Locations", schema.getClass("Attractions").getSuperClassesNames().get(1));
    assertThat(schema.getClass("Attractions").getSubclasses()).hasSize(4);
    assertThat(schema.getClass("Attractions").count()).isEqualTo(attractionsCount);

    assertThat(schema.getClass("ArchaeologicalSites")).isNotNull();
    assertEquals("V", schema.getClass("ArchaeologicalSites").getSuperClassesNames().get(0));
    assertEquals(
        "Attractions", schema.getClass("ArchaeologicalSites").getSuperClassesNames().get(1));
    assertThat(schema.getClass("ArchaeologicalSites").count()).isEqualTo(archSitesCount);

    assertThat(schema.getClass("Castles")).isNotNull();
    assertEquals("V", schema.getClass("Castles").getSuperClassesNames().get(0));
    assertEquals("Attractions", schema.getClass("Castles").getSuperClassesNames().get(1));
    assertThat(schema.getClass("Castles").count()).isEqualTo(castlesCount);

    assertThat(schema.getClass("Monuments")).isNotNull();
    assertEquals("V", schema.getClass("Monuments").getSuperClassesNames().get(0));
    assertEquals("Attractions", schema.getClass("Monuments").getSuperClassesNames().get(1));
    assertThat(schema.getClass("Monuments").count()).isEqualTo(monumentsCount);

    assertThat(schema.getClass("Theatres")).isNotNull();
    assertEquals("V", schema.getClass("Theatres").getSuperClassesNames().get(0));
    assertEquals("Attractions", schema.getClass("Theatres").getSuperClassesNames().get(1));
    assertThat(schema.getClass("Theatres").count()).isEqualTo(theatresCount);

    assertThat(schema.getClass("Services")).isNotNull();
    assertEquals("V", schema.getClass("Services").getSuperClassesNames().get(0));
    assertEquals("Locations", schema.getClass("Services").getSuperClassesNames().get(1));
    assertThat(schema.getClass("Services").getSubclasses()).hasSize(2);
    assertThat(schema.getClass("Services").count()).isEqualTo(ServicesCount);

    assertThat(schema.getClass("Hotels")).isNotNull();
    assertEquals("V", schema.getClass("Hotels").getSuperClassesNames().get(0));
    assertThat(schema.getClass("Hotels").count()).isEqualTo(hotelsCount);

    assertThat(schema.getClass("Restaurants")).isNotNull();
    assertEquals("V", schema.getClass("Restaurants").getSuperClassesNames().get(0));
    assertThat(schema.getClass("Restaurants").count()).isEqualTo(restaurantsCount);

    assertThat(schema.getClass("Profiles")).isNotNull();
    assertEquals("V", schema.getClass("Profiles").getSuperClassesNames().get(0));
    assertThat(schema.getClass("Profiles").count()).isEqualTo(profilesCount);

    assertThat(schema.getClass("Customers")).isNotNull();
    assertEquals("V", schema.getClass("Customers").getSuperClassesNames().get(0));
    assertThat(schema.getClass("Customers").count()).isEqualTo(customersCount);

    assertThat(schema.getClass("Countries")).isNotNull();
    assertEquals("V", schema.getClass("Countries").getSuperClassesNames().get(0));
    assertThat(schema.getClass("Countries").count()).isEqualTo(countriesCount);

    assertThat(schema.getClass("Orders")).isNotNull();
    assertEquals("V", schema.getClass("Orders").getSuperClassesNames().get(0));
    assertThat(schema.getClass("Orders").count()).isEqualTo(ordersCount);

    assertThat(schema.getClass("Reviews")).isNotNull();
    assertEquals("V", schema.getClass("Reviews").getSuperClassesNames().get(0));
    assertThat(schema.getClass("Reviews").count()).isEqualTo(reviewsCount);
    //

    // edges
    assertThat(schema.getClass("E")).isNotNull();
    assertThat(schema.getClass("E").getSubclasses()).hasSize(10);
    assertThat(schema.getClass("E").count()).isEqualTo(eCount);

    assertThat(schema.getClass("HasCustomer")).isNotNull();
    assertEquals("E", schema.getClass("HasCustomer").getSuperClassesNames().get(0));
    assertThat(schema.getClass("HasCustomer").count()).isEqualTo(hasCustomerCount);

    assertThat(schema.getClass("HasEaten")).isNotNull();
    assertEquals("E", schema.getClass("HasEaten").getSuperClassesNames().get(0));
    assertEquals("HasUsedService", schema.getClass("HasEaten").getSuperClassesNames().get(1));
    assertThat(schema.getClass("HasEaten").count()).isEqualTo(hasEatenCount);

    assertThat(schema.getClass("HasFriend")).isNotNull();
    assertEquals("E", schema.getClass("HasFriend").getSuperClassesNames().get(0));
    assertThat(schema.getClass("HasFriend").count()).isEqualTo(hasFriendCount);

    assertThat(schema.getClass("HasProfile")).isNotNull();
    assertEquals("E", schema.getClass("HasProfile").getSuperClassesNames().get(0));
    assertThat(schema.getClass("HasProfile").count()).isEqualTo(hasProfileCount);

    assertThat(schema.getClass("HasReview")).isNotNull();
    assertEquals("E", schema.getClass("HasReview").getSuperClassesNames().get(0));
    assertThat(schema.getClass("HasReview").count()).isEqualTo(hasReviewCount);

    assertThat(schema.getClass("HasStayed")).isNotNull();
    assertEquals("E", schema.getClass("HasStayed").getSuperClassesNames().get(0));
    assertEquals("HasUsedService", schema.getClass("HasStayed").getSuperClassesNames().get(1));
    assertThat(schema.getClass("HasStayed").count()).isEqualTo(hasStayedCount);

    assertThat(schema.getClass("HasUsedService")).isNotNull();
    assertEquals("E", schema.getClass("HasUsedService").getSuperClassesNames().get(0));
    assertThat(schema.getClass("HasUsedService").getSubclasses()).hasSize(2);
    assertThat(schema.getClass("HasUsedService").count()).isEqualTo(hasUsedServiceCount);

    // other way to check inheritance
    List<OResult> results =
        db
            .query(
                "SELECT DISTINCT(@class) AS className from `HasUsedService` ORDER BY className ASC")
            .stream()
            .collect(Collectors.toList());
    assertEquals(2, results.size());
    assertEquals("HasEaten", results.get(0).getProperty("className"));
    assertEquals("HasStayed", results.get(1).getProperty("className"));
    //

    assertThat(schema.getClass("HasVisited")).isNotNull();
    assertEquals("E", schema.getClass("HasVisited").getSuperClassesNames().get(0));
    assertThat(schema.getClass("HasVisited").count()).isEqualTo(hasVisitedCount);

    assertThat(schema.getClass("IsFromCountry")).isNotNull();
    assertEquals("E", schema.getClass("IsFromCountry").getSuperClassesNames().get(0));
    assertThat(schema.getClass("IsFromCountry").count()).isEqualTo(isFromCountryCount);

    assertThat(schema.getClass("MadeReview")).isNotNull();
    assertEquals("E", schema.getClass("MadeReview").getSuperClassesNames().get(0));
    assertThat(schema.getClass("MadeReview").count()).isEqualTo(madeReviewCount);
    //

  }

  @Test
  public void testDataModel() throws Exception {

    // all customers have a country
    OResultSet resultSet =
        db.query(
            "MATCH {class: Customers, as: customer}-IsFromCountry->{class: Countries, as: country} RETURN  customer");
    assertThat(resultSet).hasSize(customersCount);
    resultSet.close();

    // all customers have a profile
    resultSet =
        db.query(
            "MATCH {class: Customers, as: customer}-HasProfile->{class: Profiles, as: profile} RETURN customer");
    assertThat(resultSet).hasSize(customersCount);
    resultSet.close();

    // all customers have at least 1 order
    resultSet =
        db.query(
            "MATCH {class: Orders, as: order}-HasCustomer->{class: Customers, as:customer} RETURN order");
    assertThat(resultSet.stream().count()).isGreaterThan(customersCount);
    resultSet.close();
  }

  @Test
  public void testMatchWithConditionInBackTraversal() throws Exception {
    OResultSet resultSet =
        db.query(
            "MATCH \n"
                + "{class:Profiles, as:profileA} <-HasProfile- {as:customerA} -MadeReview-> {as:reviewA} <-HasReview- {as:restaurant},\n"
                + "{as:profileB, where:($matched.profileA != $currentMatch)} <-HasProfile- {as:customerB} -MadeReview-> {as:reviewB} <-HasReview- {as:restaurant}\n"
                + "return  profileA.Id as idA, profileA.Name, profileA.Surname, profileB.Id as idA, profileB.Name, profileB.Surname\n"
                + "limit 10\n");

    int size = 0;
    while (resultSet.hasNext()) {
      OResult item = resultSet.next();
      assertThat((Object) item.getProperty("idA")).isNotEqualTo(item.getProperty("idB"));
      size++;
    }
    assertThat(size).isEqualTo(10);
    resultSet.close();
  }
}
