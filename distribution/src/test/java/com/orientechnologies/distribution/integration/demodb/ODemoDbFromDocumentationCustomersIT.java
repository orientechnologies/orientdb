package com.orientechnologies.distribution.integration.demodb;

import com.orientechnologies.distribution.integration.OIntegrationTestTemplate;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import java.util.List;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Created by santo-it on 2017-08-27.
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class ODemoDbFromDocumentationCustomersIT extends OIntegrationTestTemplate {

  @Test
  public void test_Customers_Example_1() throws Exception {

    OResultSet resultSet = db.query("MATCH {class: Customers, as: c, where: (OrderedId=1)}--{as: n} \n" + "RETURN $pathelements");

    final List<OResult> results = resultSet.stream().collect(Collectors.toList());
    assertThat(results)
        .hasSize(22);

    resultSet.close();
    db.close();
  }

  @Test
  public void test_Customers_Example_2() throws Exception {

    OResultSet resultSet = db.query("MATCH {class: Customers, as: customer, where: (OrderedId=1)}--{Class: Locations} \n"
        + "RETURN $pathelements");

    final List<OResult> results = resultSet.stream().collect(Collectors.toList());
    assertThat(results)
        .hasSize(16);

    resultSet.close();
    db.close();
  }

  @Test
  public void test_Customers_Example_3() throws Exception {

    OResultSet resultSet = db.query("MATCH {class: Customers, as: c, where: (OrderedId=1)}--{class: Locations, as: loc}-HasReview->{class: Reviews, as: r, optional: true} \n"
        + "RETURN $pathelements");

    final List<OResult> results = resultSet.stream().collect(Collectors.toList());
    assertThat(results)
        .hasSize(21);

    resultSet.close();
    db.close();
  }

  @Test
  public void test_Customers_Example_4() throws Exception {

    OResultSet resultSet = db.query("MATCH {class: Customers, as: customer, where: (OrderedId=1)}--{class: Locations, as: loc}--{class: Customers, as: otherCustomers, where: (OrderedId<>1)} \n"
        + "RETURN $pathelements");

    final List<OResult> results = resultSet.stream().collect(Collectors.toList());
    assertThat(results)
        .hasSize(150);

    resultSet.close();
    db.close();
  }

  @Test
  public void test_Customers_Example_5() throws Exception {

    OResultSet resultSet = db.query("MATCH {as: n}<-HasStayed-{class: Customers, as: c, where: (OrderedId=2)} \n"
        + "RETURN $pathelements");

    final List<OResult> results = resultSet.stream().collect(Collectors.toList());
    assertThat(results)
        .hasSize(12);

    resultSet.close();
    db.close();
  }

  @Test
  public void test_Customers_Example_6() throws Exception {

    OResultSet resultSet = db.query("MATCH {as: n}<-HasEaten-{class: Customers, as: c, where: (OrderedId=1)} \n"
        + "RETURN $pathelements");

    final List<OResult> results = resultSet.stream().collect(Collectors.toList());
    assertThat(results)
        .hasSize(6);

    resultSet.close();
    db.close();
  }

  @Test
  public void test_Customers_Example_7() throws Exception {

    OResultSet resultSet = db.query("SELECT \n" + "  OrderedId as CustomerId,\n"
        + "  out(\"MadeReview\").size() AS ReviewNumbers \n" + "FROM `Customers` \n" + "ORDER BY ReviewNumbers DESC \n" + "LIMIT 3");

    final List<OResult> results = resultSet.stream().collect(Collectors.toList());
    assertThat(results)
        .hasSize(3);

    final OResult result = results.iterator().next();

    assertThat(result.<Long>getProperty("CustomerId")).isEqualTo(384);
    assertThat(result.<Integer>getProperty("ReviewNumbers")).isEqualTo(29);

    resultSet.close();
    db.close();
  }

  @Test
  public void test_Customers_Example_8() throws Exception {

    OResultSet resultSet = db.query("MATCH {class: Customers, as: c, where: (OrderedId=1)}<-HasCustomer-{class: Orders, as: o} \n"
        + "RETURN $pathelements");

    final List<OResult> results = resultSet.stream().collect(Collectors.toList());
    assertThat(results)
        .hasSize(2);

    resultSet.close();
    db.close();
  }

  @Test
  public void test_Customers_Example_9() throws Exception {

    OResultSet resultSet = db.query("SELECT sum(Amount) as TotalAmount\n" + "FROM (\n" + "  SELECT expand(in('HasCustomer'))\n"
        + "  FROM Customers\n" + "  WHERE OrderedId=2\n" + ")");

    final List<OResult> results = resultSet.stream().collect(Collectors.toList());
    assertThat(results)
        .hasSize(1);

    final OResult result = results.iterator().next();

    assertThat(result.<Long>getProperty("TotalAmount")).isEqualTo(1750);

    resultSet.close();
    db.close();
  }

  @Test
  public void test_Customers_Example_10() throws Exception {

    OResultSet resultSet = db.query("SELECT \n" + "  OrderedId as CustomerId,\n"
        + "  in(\"HasCustomer\").size() AS NumberOfOrders \n" + "FROM Customers \n" + "ORDER BY NumberOfOrders \n" + "DESC LIMIT 3");

    final List<OResult> results = resultSet.stream().collect(Collectors.toList());
    assertThat(results)
        .hasSize(3);

    final OResult result = results.iterator().next();

    assertThat(result.<Long>getProperty("CustomerId")).isEqualTo(113);
    assertThat(result.<Integer>getProperty("NumberOfOrders")).isEqualTo(6);

    resultSet.close();
    db.close();
  }

  @Test
  public void test_Customers_Example_11() throws Exception {

    OResultSet resultSet = db.query("SELECT \n" + "  Name as CountryName,\n"
        + "  in('IsFromCountry').size() as NumberOfCustomers \n" + "FROM Countries\n" + "ORDER BY NumberOfCustomers DESC \n"
        + "LIMIT 3");

    final List<OResult> results = resultSet.stream().collect(Collectors.toList());
    assertThat(results)
        .hasSize(3);

    final OResult result = results.iterator().next();

    assertThat(result.<String>getProperty("CountryName")).isEqualTo("Kyrgyzstan");
    assertThat(result.<Integer>getProperty("NumberOfCustomers")).isEqualTo(7);

    resultSet.close();
    db.close();
  }

}