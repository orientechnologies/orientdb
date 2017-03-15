package com.orientechnologies.distribution.integration;

import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import org.junit.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Created by frank on 15/03/2017.
 */
public class OCommunityEditionSingleNoteIT extends OIntegrationTestTemplate {

  @Test
  public void testSomething() throws Exception {

    List<ODocument> result = db.command(new OCommandSQL("select from Hotels limit 20")).execute();

    assertThat(result).hasSize(20);
  }
}
