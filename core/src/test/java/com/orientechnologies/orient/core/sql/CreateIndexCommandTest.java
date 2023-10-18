package com.orientechnologies.orient.core.sql;

import com.orientechnologies.BaseMemoryDatabase;
import com.orientechnologies.orient.core.index.OIndexException;
import org.junit.Test;

/** Created by tglman on 02/02/16. */
public class CreateIndexCommandTest extends BaseMemoryDatabase {

  @Test(expected = OIndexException.class)
  public void testCreateIndexOnMissingPropertyWithCollate() {
    db.getMetadata().getSchema().createClass("Test");
    db.command(new OCommandSQL(" create index Test.test on Test(test collate ci) UNIQUE"))
        .execute();
  }
}
