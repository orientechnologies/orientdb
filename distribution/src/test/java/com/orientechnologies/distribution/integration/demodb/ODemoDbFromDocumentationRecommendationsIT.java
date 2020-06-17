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

/** Created by santo-it on 2017-08-28. */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class ODemoDbFromDocumentationRecommendationsIT extends OIntegrationTestTemplate {

  @Test
  public void test_Recommendations_Example_1() throws Exception {

    OResultSet resultSet =
        db.query(
            "MATCH \n"
                + "  {class: Profiles, as: profile, where: (Name = 'Isabella' AND Surname='Gomez')}-HasFriend-{as: friend},\n"
                + "  {as: friend}-HasFriend-{as: friendOfFriend, where: ($matched.profile not in $currentMatch.both('HasFriend') and $matched.profile != $currentMatch)} \n"
                + "RETURN DISTINCT friendOfFriend.Name");

    final List<OResult> results = resultSet.stream().collect(Collectors.toList());
    assertThat(results).hasSize(29);

    resultSet.close();
    db.close();
  }

  @Test
  public void test_Recommendations_Example_2() throws Exception {

    OResultSet resultSet =
        db.query(
            "MATCH \n"
                + "  {Class: Customers, as: customer, where: (OrderedId=1)}-HasProfile->{class: Profiles, as: profile},\n"
                + "  {as: profile}-HasFriend->{class: Profiles, as: friend},\n"
                + "  {as: friend}<-HasProfile-{Class: Customers, as: customerFriend},\n"
                + "  {as: customerFriend}-HasStayed->{Class: Hotels, as: hotel},\n"
                + "  {as: customerFriend}-MadeReview->{Class: Reviews, as: review},\n"
                + "  {as: hotel}-HasReview->{as: review}\n"
                + "RETURN $pathelements");

    final List<OResult> results = resultSet.stream().collect(Collectors.toList());
    assertThat(results).hasSize(12);

    resultSet.close();
    db.close();
  }

  @Test
  public void test_Recommendations_Example_2_bis() throws Exception {

    OResultSet resultSet =
        db.query(
            "MATCH\n"
                + "  {Class: Customers, as: customer, where: (OrderedId=1)}-HasProfile->{class: Profiles, as: profile},\n"
                + "  {as: profile}-HasFriend->{class: Profiles, as: friend},\n"
                + "  {as: friend}<-HasProfile-{Class: Customers, as: customerFriend},\n"
                + "  {as: customerFriend}-HasStayed->{Class: Hotels, as: hotel},\n"
                + "  {as: customerFriend}-MadeReview->{Class: Reviews, as: review},\n"
                + "  {as: hotel}.outE('HasReview'){as: ReviewStars, where: (Stars>3)}.inV(){as: review}\n"
                + "RETURN $pathelements");

    final List<OResult> results = resultSet.stream().collect(Collectors.toList());
    assertThat(results).hasSize(7);

    resultSet.close();
    db.close();
  }
}
