package com.orientechnologies.distribution.integration.demodb;

import com.orientechnologies.distribution.integration.OIntegrationTestTemplate;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Created by frank on 24/05/2017.
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class ODemoDbFromDocumentationProfileIT extends OIntegrationTestTemplate {

  @Test
  public void test_Profile_Example_1() throws Exception {

    OResultSet resultSet = db.query("SELECT \n"
        + "  count(*) as NumberOfProfiles, \n"
        + "  Birthday.format('yyyy') AS YearOfBirth \n"
        + "FROM Profiles \n"
        + "GROUP BY YearOfBirth \n"
        + "ORDER BY NumberOfProfiles DESC");

    assertThat(resultSet)
        .hasSize(51);

    resultSet.close();
    db.close();
  }

  @Test
  public void test_Profile_Example_2() throws Exception {

    OResultSet resultSet = db.query(
        "SELECT  @rid as Profile_RID, Name, Surname, (out('HasFriend').size() + in('HasFriend').size()) AS FriendsNumber "
            + "FROM `Profiles` "
            + "ORDER BY FriendsNumber DESC LIMIT 3");

    assertThat(resultSet)
        .hasSize(3);

    resultSet.close();
    db.close();
  }

}