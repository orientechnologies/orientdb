package com.orientechnologies.distribution.integration;

import com.orientechnologies.orient.core.sql.executor.OResultSet;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * Created by frank on 15/03/2017.
 */
@Test
public class OCommunityEditionSingleNodeIT extends OIntegrationTestTemplate {

  @Test
  public void testSearchOnField() throws Exception {

    OResultSet result = db.query("SELECT from ArchaeologicalSites where search_fields(['Name'],'foro') = true");
    Assert.assertEquals(result.stream().count(), 2);
    result.close();
  }

  @Test
  public void testSearchOnClass() throws Exception {

    OResultSet result = db.query("select * from `Hotels` where search_class('western')=true");
    Assert.assertEquals(result.stream().count(), 6);
    result.close();
  }

}
