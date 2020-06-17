package com.orientechnologies.distribution.integration.demodb;

import static org.assertj.core.api.Assertions.assertThat;

import com.orientechnologies.distribution.integration.OIntegrationTestTemplate;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import java.util.List;
import java.util.stream.Collectors;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

/** Created by frank on 24/05/2017. */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class ODemoDbFromDocumentationProfileIT extends OIntegrationTestTemplate {

  @Test
  public void test_Profile_Example_1() throws Exception {

    OResultSet resultSet =
        db.query(
            "SELECT \n"
                + "  count(*) as NumberOfProfiles, \n"
                + "  Birthday.format('yyyy') AS YearOfBirth \n"
                + "FROM Profiles \n"
                + "GROUP BY YearOfBirth \n"
                + "ORDER BY NumberOfProfiles DESC");

    final List<OResult> results = resultSet.stream().collect(Collectors.toList());
    assertThat(results).hasSize(51);

    final OResult result = results.iterator().next();

    assertThat(result.<Long>getProperty("NumberOfProfiles")).isEqualTo(34);
    assertThat(result.<String>getProperty("YearOfBirth")).isEqualTo("1997");

    resultSet.close();
    db.close();
  }

  @Test
  public void test_Profile_Example_2() throws Exception {

    OResultSet resultSet =
        db.query(
            "SELECT  @rid as Profile_RID, Name, Surname, (both('HasFriend').size()) AS FriendsNumber "
                + "FROM `Profiles` "
                + "ORDER BY FriendsNumber DESC LIMIT 3");

    final List<OResult> results = resultSet.stream().collect(Collectors.toList());
    assertThat(results).hasSize(3);

    final OResult result = results.iterator().next();

    assertThat(result.<String>getProperty("Name")).isEqualTo("Jeremiah");
    assertThat(result.<String>getProperty("Surname")).isEqualTo("Schneider");
    assertThat(result.<Integer>getProperty("FriendsNumber")).isEqualTo(12);

    resultSet.close();
    db.close();
  }
}
