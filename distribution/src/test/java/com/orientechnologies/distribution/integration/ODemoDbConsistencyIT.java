package com.orientechnologies.distribution.integration;

import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import org.junit.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;

/**
 * Created by frank on 15/03/2017.
 */
public class ODemoDbConsistencyIT extends OIntegrationTestTemplate {

  private int customerNumber     = 400;
  private int hotelNumbers       = 1154;
  private int restaurantsNumbers = 1951;
  private int casteNumbers       = 127;
  private int archSiteNumbers    = 55;
  private int monumentNumbers    = 137;
  private int theatreNumbers     = 117;

  @Test
  public void testMetadata() throws Exception {

    OSchema schema = db.getMetadata().getSchema();

    // vertex class existence    
    assertThat(schema.getClass("V")).isNotNull();
    assertThat(schema.getClass("V").getSubclasses()).hasSize(14);
    //assertThat(schema.getClass("V").count()).isEqualTo();

    assertThat(schema.getClass("Locations")).isNotNull();
    assertThat(schema.getClass("Locations").count()).isEqualTo(3541);

    assertThat(schema.getClass("Attractions")).isNotNull();
    assertThat(schema.getClass("Attractions").count()).isEqualTo(436);

    assertThat(schema.getClass("Castles")).isNotNull();
    assertThat(schema.getClass("Castles").count()).isEqualTo(casteNumbers);

    assertThat(schema.getClass("Theatres")).isNotNull();
    assertThat(schema.getClass("Theatres").count()).isEqualTo(theatreNumbers);

    assertThat(schema.getClass("Monuments")).isNotNull();
    assertThat(schema.getClass("Monuments").count()).isEqualTo(monumentNumbers);

    assertThat(schema.getClass("ArchaeologicalSites")).isNotNull();
    assertThat(schema.getClass("ArchaeologicalSites").count()).isEqualTo(archSiteNumbers);

    assertThat(schema.getClass("Services")).isNotNull();
    assertThat(schema.getClass("Services").count()).isEqualTo(3105);

    assertThat(schema.getClass("Hotels")).isNotNull();
    assertThat(schema.getClass("Hotels").count()).isEqualTo(hotelNumbers);

    assertThat(schema.getClass("Restaurants")).isNotNull();
    assertThat(schema.getClass("Restaurants").count()).isEqualTo(restaurantsNumbers);

    assertThat(schema.getClass("Profiles")).isNotNull();
    //assertEquals(0, db.getMetadata().getSchema().getClass("Profiles").count());

    assertThat(schema.getClass("Customers")).isNotNull();
    assertThat(schema.getClass("Customers").count()).isEqualTo(customerNumber);

    assertThat(schema.getClass("Orders")).isNotNull();
    //assertEquals(0, db.getMetadata().getSchema().getClass("Orders").count());

    assertThat(schema.getClass("Countries")).isNotNull();
    assertThat(schema.getClass("Countries").count()).isEqualTo(249);

    assertThat(schema.getClass("Reviews")).isNotNull();
    //assertEquals(0, db.getMetadata().getSchema().getClass("Reviews").count());

    // edge class existence
    assertThat(schema.getClass("E")).isNotNull();
    assertThat(schema.getClass("E").getSubclasses()).hasSize(10);
    //assertThat(schema.getClass("E").count()).isEqualTo();

    assertThat(schema.getClass("HasCustomer")).isNotNull();
    assertThat(schema.getClass("HasVisited")).isNotNull();
    assertThat(schema.getClass("IsFromCountry")).isNotNull();
    assertThat(schema.getClass("HasStayed")).isNotNull();
    assertThat(schema.getClass("MadeReview")).isNotNull();
    assertThat(schema.getClass("HasReview")).isNotNull();
    assertThat(schema.getClass("HasEaten")).isNotNull();
    assertThat(schema.getClass("HasProfile")).isNotNull();
    assertThat(schema.getClass("HasFriend")).isNotNull();
    assertThat(schema.getClass("HasUsedService")).isNotNull();

    // Vertex inheritance
    // to do

    // Edge inheritance
    // method 1
    assertEquals("E", schema.getClass("HasEaten").getSuperClassesNames().get(0));
    assertEquals("HasUsedService", schema.getClass("HasEaten").getSuperClassesNames().get(1));
    assertEquals("E", schema.getClass("HasStayed").getSuperClassesNames().get(0));
    assertEquals("HasUsedService", schema.getClass("HasStayed").getSuperClassesNames().get(1));

    //method 2

    List<ODocument> results = db
        .query(new OSQLSynchQuery<ODocument>("SELECT DISTINCT(@class) AS className from `HasUsedService` ORDER BY className ASC"));
    assertEquals(2, results.size());
    assertEquals("HasEaten", results.get(0).field("className"));
    assertEquals("HasStayed", results.get(1).field("className"));

    //method 3 - fails, to ask Luigi why
    //sqlQuery = "SELECT COUNT(*) as edgeCount, DISTINCT(@class) AS edgeClassName from `HasUsedService` GROUP BY edgeClassName ORDER BY edgeClassName ASC";
    //qResult = db.command(new OCommandSQL(sqlQuery)).execute();
    //assertEquals(2,qResult.size());
    //assertEquals( "HasEaten", qResult.get(0).field("className"));
    //assertEquals( "HasStayed", qResult.get(1).field("className"));

  }

  @Test
  public void testMatchQueries() throws Exception {

    // all customers have a country
    List<ODocument> resultSet = db
        .query(new OSQLSynchQuery<ODocument>(
            "MATCH {class: Customers, as: customer}-IsFromCountry->{class: Countries, as: country} RETURN  customer"));
    assertThat(resultSet).hasSize(customerNumber);

    // all customers have a profile
    resultSet = db.query(
        new OSQLSynchQuery<ODocument>(
            "MATCH {class: Customers, as: customer}-HasProfile->{class: Profiles, as: profile} RETURN customer"));
    assertThat(resultSet).hasSize(customerNumber);

    // all customers have at least 1 order
    resultSet = db
        .query(new OSQLSynchQuery<ODocument>(
            "MATCH {class: Orders, as: order}-HasCustomer->{class: Customers, as:customer} RETURN order"));
    assertThat(resultSet.size()).isGreaterThan(customerNumber);

  }
}
