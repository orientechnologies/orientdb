package com.orientechnologies.orient.core.sql;

import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.testng.AssertJUnit.assertNotNull;

public class SQLAlterClassTest {

  @Test
  public void alterClassRenameTest() {
    ODatabaseDocument db = new ODatabaseDocumentTx("memory:" + SQLAlterClassTest.class.getName());
    db.create();
    try {
      db.getMetadata().getSchema().createClass("TestClass");

      try {
        db.command(new OCommandSQL("alter class TestClass name = 'test_class'")).execute();
        Assert.fail("the rename should fail for wrong syntax");
      } catch (OCommandSQLParsingException ex) {

      }
      assertNotNull(db.getMetadata().getSchema().getClass("TestClass"));

    } finally {
      db.drop();
    }
  }

}
