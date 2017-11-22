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
public class ODemoDbFromDocumentationProfileIT extends OIntegrationTestTemplate {

  @Test(priority = 1)
  public void test_Profile_Example_1() throws Exception {

    OResultSet resultSet = db.query(
        "SELECT \n" + "  count(*) as NumberOfProfiles, \n" + "  Birthday.format('yyyy') AS YearOfBirth \n" + "FROM Profiles \n"
            + "GROUP BY YearOfBirth \n" + "ORDER BY NumberOfProfiles DESC");

    final List<OResult> results = resultSet.stream().collect(Collectors.toList());
    Assert.assertEquals(results.size(), 51);

    final OResult result = results.iterator().next();

    Assert.assertEquals(result.<Long>getProperty("NumberOfProfiles"), Long.valueOf(34));
    Assert.assertEquals(result.getProperty("YearOfBirth"), "1997");

    resultSet.close();
  }

  @Test(priority = 2)
  public void test_Profile_Example_2() throws Exception {
    OResultSet resultSet = db.query(
        "SELECT  @rid as Profile_RID, Name, Surname, (both('HasFriend').size()) AS FriendsNumber " + "FROM `Profiles` "
            + "ORDER BY FriendsNumber DESC LIMIT 3");

    final List<OResult> results = resultSet.stream().collect(Collectors.toList());
    Assert.assertEquals(results.size(), 3);
    final OResult result = results.iterator().next();

    Assert.assertEquals(result.getProperty("Name"), "Jeremiah");
    Assert.assertEquals(result.getProperty("Surname"), "Schneider");
    Assert.assertEquals(result.<Integer>getProperty("FriendsNumber"), Integer.valueOf(12));

    resultSet.close();
  }
}