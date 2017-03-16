package com.orientechnologies.distribution.integration;

import com.orientechnologies.orient.core.sql.executor.OResultSet;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Created by frank on 15/03/2017.
 */
public class OCommunityEditionSingleNoteIT extends OIntegrationTestTemplate {

  @Test
  public void testSomething() throws Exception {

    OResultSet result = db.query("select from Hotels limit 20");

    assertThat(result).hasSize(20);
  }
}
