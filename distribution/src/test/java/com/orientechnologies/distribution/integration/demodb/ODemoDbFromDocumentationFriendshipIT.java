package com.orientechnologies.distribution.integration.demodb;

import com.orientechnologies.distribution.integration.OIntegrationTestTemplate;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.List;
import java.util.stream.Collectors;
/**
 * Created by frank on 24/05/2017.
 */
@Test
public class ODemoDbFromDocumentationFriendshipIT extends OIntegrationTestTemplate {
  @Test(priority = 1)
  public void test_Friendship_Example_1() throws Exception {

    OResultSet resultSet = db.query(
        "MATCH {Class: Profiles, as: profile, where: (Name='Santo' AND Surname='OrientDB')}-HasFriend-{Class: Profiles, as: friend} \n"
            + "RETURN $pathelements");

    Assert.assertEquals(resultSet.stream().count(), 20);
    resultSet.close();

    resultSet = db.query("SELECT \n" + "  both('HasFriend').size() AS FriendsNumber \n" + "FROM `Profiles` \n"
        + "WHERE Name='Santo' AND Surname='OrientDB'");

    final List<OResult> results = resultSet.stream().collect(Collectors.toList());
    Assert.assertEquals(results.size(), 1);

    final OResult result = results.iterator().next();
    Assert.assertEquals(result.<Integer>getProperty("FriendsNumber"), Integer.valueOf(10));

    resultSet.close();
  }

  @Test(priority = 2)
  public void test_Friendship_Example_2() throws Exception {

    OResultSet resultSet = db.query(
        "MATCH {Class: Profiles, as: profile, where: (Name='Santo' AND Surname='OrientDB')}-HasFriend-{Class: Profiles, as: friend}<-HasProfile-{class: Customers, as: customer}\n"
            + "RETURN $pathelements");

    Assert.assertEquals(resultSet.stream().count(), 15);

    resultSet.close();
  }

  @Test(priority = 3)
  public void test_Friendship_Example_3() throws Exception {

    OResultSet resultSet = db.query(
        "MATCH {Class: Profiles, as: profile, where: (Name='Santo' AND Surname='OrientDB')}-HasFriend-{Class: Profiles, as: friend}<-HasProfile-{class: Customers, as: customer}-IsFromCountry->{Class: Countries, as: country}\n"
            + "RETURN $pathelements");

    Assert.assertEquals(resultSet.stream().count(), 20);

    resultSet.close();
  }

  @Test(priority = 4)
  public void test_Friendship_Example_4() throws Exception {

    OResultSet resultSet = db.query(
        "MATCH {Class: Profiles, as: profile, where: (Name='Santo' AND Surname='OrientDB')}-HasFriend-{Class: Profiles, as: friend}<-HasProfile-{class: Customers, as: customer}<-HasCustomer-{Class: Orders, as: order} \n"
            + "RETURN $pathelements");

    Assert.assertEquals(resultSet.stream().count(), 40);

    resultSet.close();
  }

  @Test(priority = 5)
  public void test_Friendship_Example_5() throws Exception {

    OResultSet resultSet = db.query(
        "SELECT \n" + "  OrderedId as Customer_OrderedId, \n" + "  in('HasCustomer').size() as NumberOfOrders, \n"
            + "  out('HasProfile').Name as Friend_Name, \n" + "  out('HasProfile').Surname as Friend_Surname \n" + "FROM (\n"
            + "  SELECT expand(customer) \n" + "  FROM (\n"
            + "    MATCH {Class: Profiles, as: profile, where: (Name='Santo' AND Surname='OrientDB')}-HasFriend-{Class: Profiles, as: friend}<-HasProfile-{class: Customers, as: customer} \n"
            + "    RETURN customer\n" + "  )\n" + ") \n" + "ORDER BY NumberOfOrders DESC \n" + "LIMIT 3");

    final List<OResult> results = resultSet.stream().collect(Collectors.toList());
    Assert.assertEquals(results.size(), 3);

    final OResult result = results.iterator().next();

    Assert.assertEquals(result.<Long>getProperty("Customer_OrderedId"), Long.valueOf(4));
    Assert.assertEquals(result.<Integer>getProperty("NumberOfOrders"), Integer.valueOf(4));

    resultSet.close();
  }

  @Test(priority = 6)
  public void test_Friendship_Example_6() throws Exception {

    OResultSet resultSet = db.query(
        "SELECT \n" + "  OrderedId as Customer_OrderedId, \n" + "  out('HasVisited').size() as NumberOfVisits, \n"
            + "  out('HasProfile').Name as Friend_Name, \n" + "  out('HasProfile').Surname as Friend_Surname \n" + "FROM (\n"
            + "  SELECT expand(customer) \n" + "  FROM (\n"
            + "    MATCH {Class: Profiles, as: profile, where: (Name='Santo' AND Surname='OrientDB')}-HasFriend-{Class: Profiles, as: friend}<-HasProfile-{class: Customers, as: customer} \n"
            + "    RETURN customer\n" + "  )\n" + ") \n" + "ORDER BY NumberOfVisits DESC \n" + "LIMIT 3");

    final List<OResult> results = resultSet.stream().collect(Collectors.toList());
    Assert.assertEquals(results.size(), 3);

    final OResult result = results.iterator().next();

    Assert.assertEquals(result.<Long>getProperty("Customer_OrderedId"), Long.valueOf(2));
    Assert.assertEquals(result.<Integer>getProperty("NumberOfVisits"), Integer.valueOf(23));

    resultSet.close();
  }

  @Test(priority = 7)
  public void test_Friendship_Example_7() throws Exception {

    OResultSet resultSet = db.query(
        "SELECT \n" + "  @Rid as Friend_RID, \n" + "  Name as Friend_Name, \n" + "  Surname as Friend_Surname \n" + "FROM (\n"
            + "  SELECT expand(customerFriend) \n" + "  FROM (\n"
            + "    MATCH {Class:Customers, as: customer, where:(OrderedId=1)}-HasProfile-{Class:Profiles, as: profile}-HasFriend-{Class:Profiles, as: customerFriend} RETURN customerFriend\n"
            + "  )\n" + ") \n" + "WHERE in('HasProfile').size()=0\n" + "ORDER BY Friend_Name ASC");

    final List<OResult> results = resultSet.stream().collect(Collectors.toList());
    Assert.assertEquals(results.size(), 5);

    final OResult result = results.iterator().next();

    Assert.assertEquals(result.getProperty("Friend_Name"), "Emanuele");
    Assert.assertEquals(result.getProperty("Friend_Surname"), "OrientDB");

    resultSet.close();
  }
}
