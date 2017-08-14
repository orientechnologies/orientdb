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
public class ODemoDbFromDocumentationFriendShipIT extends OIntegrationTestTemplate {

  @Test
  public void test_Friendship_Example_1() throws Exception {

    OResultSet resultSet = db.query(

        "MATCH {Class: Profiles, as: profile, where: (Name='Santo' AND Surname='OrientDB')}-HasFriend-{Class: Profiles, as: friend}  RETURN $pathelements"
    );

    assertThat(resultSet)
        .hasSize(20);

    resultSet.close();
    db.close();

  }
}