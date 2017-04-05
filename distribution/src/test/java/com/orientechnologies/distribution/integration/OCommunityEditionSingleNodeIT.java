package com.orientechnologies.distribution.integration;

import com.orientechnologies.orient.core.sql.query.OSQLAsynchQuery;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import org.junit.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Created by frank on 15/03/2017.
 */
public class OCommunityEditionSingleNodeIT extends OIntegrationTestTemplate {

  @Test
  public void testSearchOnField() throws Exception {

    List<?> result = db.query(new OSQLAsynchQuery("SELECT from ArchaeologicalSites where Name LUCENE 'foro'"));

    assertThat(result).hasSize(2);
  }

  @Test
  public void testSearchOnClass() throws Exception {

    List<?> result = db.query(new OSQLSynchQuery("select * from Hotels where Name LUCENE 'western'"));

    assertThat(result).hasSize(6);
  }

}
