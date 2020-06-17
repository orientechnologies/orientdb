package com.orientechnologies.distribution.integration;

import static org.assertj.core.api.Assertions.assertThat;

import com.orientechnologies.orient.core.sql.executor.OResultSet;
import org.junit.Test;

/** Created by frank on 15/03/2017. */
public class OCommunityEditionSingleNodeIT extends OIntegrationTestTemplate {

  @Test
  public void testSearchOnField() {

    OResultSet result =
        db.query("SELECT from ArchaeologicalSites where search_fields(['Name'],'foro') = true");

    assertThat(result).hasSize(2);
    result.close();
  }

  @Test
  public void testSearchOnClass() {

    OResultSet result = db.query("select * from `Hotels` where search_class('western')=true");

    assertThat(result).hasSize(6);
    result.close();
  }
}
