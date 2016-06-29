package com.orientechnologies.orient.core.sql;

import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import org.junit.Assert; import org.junit.Test;

import java.util.List;

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
      Assert.assertNotNull(db.getMetadata().getSchema().getClass("TestClass"));

    } finally {
      db.drop();
    }
  }

  @Test
  public void testQuoted(){
    ODatabaseDocument db = new ODatabaseDocumentTx("memory:" + SQLAlterClassTest.class.getName()+"_Quoted");
    db.create();
    try {
      try {
        db.command(new OCommandSQL("create class `Client-Type`")).execute();
        db.command(new OCommandSQL("alter class `Client-Type` addcluster `client-type_usa`")).execute();
        db.command(new OCommandSQL("insert into `Client-Type` set foo = 'bar'")).execute();
        List<?> result = db.query(new OSQLSynchQuery<Object>("Select from `Client-Type`"));
        Assert.assertEquals(result.size(), 1);
      } catch (OCommandSQLParsingException ex) {
        Assert.fail();
      }
    } finally {
      db.drop();
    }
  }


}
