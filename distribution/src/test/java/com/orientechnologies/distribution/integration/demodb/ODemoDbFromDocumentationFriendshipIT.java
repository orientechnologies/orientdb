package com.orientechnologies.distribution.integration.demodb;

import com.orientechnologies.distribution.integration.OIntegrationTestTemplate;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Created by frank on 24/05/2017.
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class ODemoDbFromDocumentationFriendshipIT extends OIntegrationTestTemplate {

  @Test
  public void test_Friendship_Example_1() throws Exception {

    OResultSet resultSet = db.query("MATCH {Class: Profiles, as: profile, where: (Name='Santo' AND Surname='OrientDB')}-HasFriend-{Class: Profiles, as: friend} \n"
        + "RETURN $pathelements");

    assertThat(resultSet)
        .hasSize(20);

    resultSet.close();

    resultSet = db.query("SELECT \n" + "  both('HasFriend').size() AS FriendsNumber \n" + "FROM `Profiles` \n"
        + "WHERE Name='Santo' AND Surname='OrientDB'");

    final List<OResult> results = resultSet.stream().collect(Collectors.toList());
    assertThat(results)
        .hasSize(1);

    final OResult result = results.iterator().next();

    assertThat(result.<Integer>getProperty("FriendsNumber")).isEqualTo(10);

    resultSet.close();
    db.close();

  }

  @Test
  public void test_Friendship_Example_2() throws Exception {

    OResultSet resultSet = db.query("MATCH {Class: Profiles, as: profile, where: (Name='Santo' AND Surname='OrientDB')}-HasFriend-{Class: Profiles, as: friend}<-HasProfile-{class: Customers, as: customer}\n"
        + "RETURN $pathelements");

    assertThat(resultSet)
        .hasSize(15);

    resultSet.close();
    db.close();

  }

  @Test
  public void test_Friendship_Example_3() throws Exception {

    OResultSet resultSet = db.query("MATCH {Class: Profiles, as: profile, where: (Name='Santo' AND Surname='OrientDB')}-HasFriend-{Class: Profiles, as: friend}<-HasProfile-{class: Customers, as: customer}-IsFromCountry->{Class: Countries, as: country}\n"
        + "RETURN $pathelements");

    assertThat(resultSet)
        .hasSize(20);

    resultSet.close();
    db.close();

  }

  @Test
  public void test_Friendship_Example_4() throws Exception {

    OResultSet resultSet = db.query("MATCH {Class: Profiles, as: profile, where: (Name='Santo' AND Surname='OrientDB')}-HasFriend-{Class: Profiles, as: friend}<-HasProfile-{class: Customers, as: customer}<-HasCustomer-{Class: Orders, as: order} \n"
        + "RETURN $pathelements");

    assertThat(resultSet)
        .hasSize(40);

    resultSet.close();
    db.close();

  }

  @Test
  public void test_Friendship_Example_5() throws Exception {

    OResultSet resultSet = db.query("SELECT \n" + "  OrderedId as Customer_OrderedId, \n"
        + "  in('HasCustomer').size() as NumberOfOrders, \n" + "  out('HasProfile').Name as Friend_Name, \n"
        + "  out('HasProfile').Surname as Friend_Surname \n" + "FROM (\n" + "  SELECT expand(customer) \n" + "  FROM (\n"
        + "    MATCH {Class: Profiles, as: profile, where: (Name='Santo' AND Surname='OrientDB')}-HasFriend-{Class: Profiles, as: friend}<-HasProfile-{class: Customers, as: customer} \n"
        + "    RETURN customer\n" + "  )\n" + ") \n" + "ORDER BY NumberOfOrders DESC \n" + "LIMIT 3");

    final List<OResult> results = resultSet.stream().collect(Collectors.toList());
    assertThat(results)
        .hasSize(3);

    final OResult result = results.iterator().next();

    assertThat(result.<Long>getProperty("Customer_OrderedId")).isEqualTo(4);
    assertThat(result.<Integer>getProperty("NumberOfOrders")).isEqualTo(4);

    resultSet.close();
    db.close();

  }

  @Test
  public void test_Friendship_Example_6() throws Exception {

    OResultSet resultSet = db.query("SELECT \n" + "  OrderedId as Customer_OrderedId, \n"
        + "  out('HasVisited').size() as NumberOfVisits, \n" + "  out('HasProfile').Name as Friend_Name, \n"
        + "  out('HasProfile').Surname as Friend_Surname \n" + "FROM (\n" + "  SELECT expand(customer) \n" + "  FROM (\n"
        + "    MATCH {Class: Profiles, as: profile, where: (Name='Santo' AND Surname='OrientDB')}-HasFriend-{Class: Profiles, as: friend}<-HasProfile-{class: Customers, as: customer} \n"
        + "    RETURN customer\n" + "  )\n" + ") \n" + "ORDER BY NumberOfVisits DESC \n" + "LIMIT 3");

    final List<OResult> results = resultSet.stream().collect(Collectors.toList());
    assertThat(results)
        .hasSize(3);

    final OResult result = results.iterator().next();

    assertThat(result.<Long>getProperty("Customer_OrderedId")).isEqualTo(2);
    assertThat(result.<Integer>getProperty("NumberOfVisits")).isEqualTo(23);

    resultSet.close();
    db.close();

  }

  @Test
  public void test_Friendship_Example_7() throws Exception {

    OResultSet resultSet = db.query("SELECT \n" + "  @Rid as Friend_RID, \n" + "  Name as Friend_Name, \n"
        + "  Surname as Friend_Surname \n" + "FROM (\n" + "  SELECT expand(customerFriend) \n" + "  FROM (\n"
        + "    MATCH {Class:Customers, as: customer, where:(OrderedId=1)}-HasProfile-{Class:Profiles, as: profile}-HasFriend-{Class:Profiles, as: customerFriend} RETURN customerFriend\n"
        + "  )\n" + ") \n" + "WHERE in('HasProfile').size()=0\n" + "ORDER BY Friend_Name ASC");

    final List<OResult> results = resultSet.stream().collect(Collectors.toList());
    assertThat(results)
        .hasSize(5);

    final OResult result = results.iterator().next();

    assertThat(result.<String>getProperty("Friend_Name")).isEqualTo("Emanuele");
    assertThat(result.<String>getProperty("Friend_Surname")).isEqualTo("OrientDB");

    resultSet.close();
    db.close();

  }

}
