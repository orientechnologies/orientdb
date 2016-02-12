package com.orientechnologies.orient.core.sql;

import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.index.OIndexException;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Created by tglman on 02/02/16.
 */
public class CreateIndexCommandTest {

  private ODatabaseDocument database;

  @BeforeMethod
  public void before() {
    database = new ODatabaseDocumentTx("memory:" + CreateIndexCommandTest.class.getSimpleName());
    database.create();
  }

  @AfterMethod
  public void after() {
    database.drop();
  }

  @Test(expectedExceptions = OIndexException.class)
  public void testCreateIndexOnMissingPropertyWithCollate() {
    database.getMetadata().getSchema().createClass("Test");
    database.command(new OCommandSQL(" create index Test.test on Test(test collate ci) UNIQUE")).execute();
  }

}
