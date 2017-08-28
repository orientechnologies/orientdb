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
 * Created by santo-it on 2017-08-28.
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class ODemoDbFromDocumentationBusinessOpportunitiesIT extends OIntegrationTestTemplate {

  @Test
  public void test_BusinessOpportunities_Example_1() throws Exception {

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

  @Test
  public void test_BusinessOpportunities_Example_2() throws Exception {

    OResultSet resultSet = db.query("SELECT DISTINCT * FROM (\n" + "  SELECT expand(customerFriend) \n" + "  FROM ( \n"
        + "    MATCH \n"
        + "      {Class:Customers, as: customer}-HasProfile-{Class:Profiles, as: profile}-HasFriend-{Class:Profiles, as: customerFriend} \n"
        + "    RETURN customerFriend\n" + "  )\n" + ") \n" + "WHERE in('HasProfile').size()=0");

    final List<OResult> results = resultSet.stream().collect(Collectors.toList());
    assertThat(results)
        .hasSize(376);

    resultSet.close();
    db.close();
  }

}
