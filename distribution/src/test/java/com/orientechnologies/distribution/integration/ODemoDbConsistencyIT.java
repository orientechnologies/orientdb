package com.orientechnologies.distribution.integration;

import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import org.assertj.core.api.Assertions;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * Created by frank on 15/03/2017.
 */
public class ODemoDbConsistencyIT extends OIntegrationTestTemplate {

  @Test
  public void testMetadta() throws Exception {

    int hotelNumbers = 1154;
    int restaurantsNumbers = 1951;

    int casteNumbers = 127;
    int archSiteNumbers = 55;
    int monumentNumbers = 137;
    int theatreNumbers = 117;

    List<ODocument> qResult = null;
    String sqlQuery = "";

    // total number of vertices
    //assertEquals(0, db.getMetadata().getSchema().getClass("V").count());

    // total number of edgex
    //assertEquals(0, db.getMetadata().getSchema().getClass("E").count());

    // vertex class existence
    OSchema schema = db.getMetadata().getSchema();
    Assertions.assertThat(schema.getClass("V")).isNotNull();
    assertEquals(14, schema.getClass("V").getSubclasses().size());

    Assertions.assertThat(schema.getClass("Locations")).isNotNull();
    assertEquals(3541, schema.getClass("Locations").count());

    Assertions.assertThat(schema.getClass("Attractions")).isNotNull();
    assertEquals(436, schema.getClass("Attractions").count());

    Assertions.assertThat(schema.getClass("Castles")).isNotNull();
    assertEquals(casteNumbers, schema.getClass("Castles").count());

    Assertions.assertThat(schema.getClass("Theatres")).isNotNull();
    assertEquals(theatreNumbers, schema.getClass("Theatres").count());

    Assertions.assertThat(schema.getClass("Monuments")).isNotNull();
    assertEquals(monumentNumbers, schema.getClass("Monuments").count());

    Assertions.assertThat(schema.getClass("ArchaeologicalSites")).isNotNull();
    assertEquals(archSiteNumbers, schema.getClass("ArchaeologicalSites").count());

    Assertions.assertThat(schema.getClass("Services")).isNotNull();
    assertEquals(3105, schema.getClass("Services").count());

    Assertions.assertThat(schema.getClass("Hotels")).isNotNull();
    assertEquals(hotelNumbers, schema.getClass("Hotels").count());

    Assertions.assertThat(schema.getClass("Restaurants")).isNotNull();
    assertEquals(restaurantsNumbers, schema.getClass("Restaurants").count());

    Assertions.assertThat(schema.getClass("Profiles")).isNotNull();
    //assertEquals(0, db.getMetadata().getSchema().getClass("Profiles").count());

    Assertions.assertThat(schema.getClass("Customers")).isNotNull();
    assertEquals(400, schema.getClass("Customers").count());

    Assertions.assertThat(schema.getClass("Orders")).isNotNull();
    //assertEquals(0, db.getMetadata().getSchema().getClass("Orders").count());

    Assertions.assertThat(schema.getClass("Countries")).isNotNull();
    assertEquals(249, schema.getClass("Countries").count());

    Assertions.assertThat(schema.getClass("Reviews")).isNotNull();
    //assertEquals(0, db.getMetadata().getSchema().getClass("Reviews").count());

    // edge class existence
    Assertions.assertThat(schema.getClass("E")).isNotNull();
    assertEquals(10, schema.getClass("E").getSubclasses().size());

    Assertions.assertThat(schema.getClass("HasCustomer")).isNotNull();
    Assertions.assertThat(schema.getClass("HasVisited")).isNotNull();
    Assertions.assertThat(schema.getClass("IsFromCountry")).isNotNull();
    Assertions.assertThat(schema.getClass("HasStayed")).isNotNull();
    Assertions.assertThat(schema.getClass("MadeReview")).isNotNull();
    Assertions.assertThat(schema.getClass("HasReview")).isNotNull();
    Assertions.assertThat(schema.getClass("HasEaten")).isNotNull();
    Assertions.assertThat(schema.getClass("HasProfile")).isNotNull();
    Assertions.assertThat(schema.getClass("HasFriend")).isNotNull();
    Assertions.assertThat(schema.getClass("HasUsedService")).isNotNull();

    // Vertex inheritance
    // to do

    // Edge inheritance
    // method 1
    assertEquals("E", schema.getClass("HasEaten").getSuperClassesNames().get(0));
    assertEquals("HasUsedService", schema.getClass("HasEaten").getSuperClassesNames().get(1));
    assertEquals("E", schema.getClass("HasStayed").getSuperClassesNames().get(0));
    assertEquals("HasUsedService", schema.getClass("HasStayed").getSuperClassesNames().get(1));

    //method 2
    sqlQuery = "SELECT DISTINCT(@class) AS className from `HasUsedService` ORDER BY className ASC";
    qResult = db.command(new OCommandSQL(sqlQuery)).execute();
    assertEquals(2, qResult.size());
    assertEquals("HasEaten", qResult.get(0).field("className"));
    assertEquals("HasStayed", qResult.get(1).field("className"));

    //method 3 - fails, to ask Luigi why
    //sqlQuery = "SELECT COUNT(*) as edgeCount, DISTINCT(@class) AS edgeClassName from `HasUsedService` GROUP BY edgeClassName ORDER BY edgeClassName ASC";
    //qResult = db.command(new OCommandSQL(sqlQuery)).execute();
    //assertEquals(2,qResult.size());
    //assertEquals( "HasEaten", qResult.get(0).field("className"));
    //assertEquals( "HasStayed", qResult.get(1).field("className"));

  }

  @Test
  public void testMatchQueries() throws Exception {

    List<ODocument> qResult = null;
    String sqlQuery = "";

    // MATCH QUERIES TESTS
    // all customers have a country
    sqlQuery = "MATCH {class: Customers, as: customer}-IsFromCountry->{class: Countries, as: country} RETURN customer";
    qResult = db.command(new OCommandSQL(sqlQuery)).execute();
    assertEquals(400, qResult.size());

    // all customers have a profile
    sqlQuery = "MATCH {class: Customers, as: customer}-HasProfile->{class: Profiles, as: profile} RETURN customer";
    qResult = db.command(new OCommandSQL(sqlQuery)).execute();
    assertEquals(400, qResult.size());

    // all customers have at least 1 order
    sqlQuery = "MATCH {class: Orders, as: order}-HasCustomer->{class: Customers, as:customer} RETURN order";
    qResult = db.command(new OCommandSQL(sqlQuery)).execute();
    assertEquals(true, qResult.size() >= 60);

  }
}
