package com.orientechnologies.orient.graph;

import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import com.orientechnologies.orient.core.command.script.OCommandScript;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OClass.INDEX_TYPE;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import com.orientechnologies.orient.core.storage.ORecordDuplicatedException;

public class TestGraphTransactionOnBatch {

  @Test
  public void testDuplicateRollback() {
    ODatabaseDocument db = new ODatabaseDocumentTx("memory:" + TestGraphTransactionOnBatch.class.getSimpleName());
    try {
      db.create();
      db.getMetadata().getSchema().createClass("E");
      OClass V = db.getMetadata().getSchema().createClass("V");
      OClass clazz = db.getMetadata().getSchema().createClass("Test");
      clazz.setSuperClass(V);
      clazz.createProperty("id", OType.STRING).createIndex(INDEX_TYPE.UNIQUE);
      try {
        db.command(
            new OCommandScript(
                "sql",
                "BEGIN \n LET a = create vertex Test SET id = \"12345678\" \n LET b = create vertex Test SET id = \"4kkrPhGe\" \n LET c =create vertex Test SET id = \"4kkrPhGe\" \n RETURN $b \n COMMIT"))
            .execute();
        Assert.fail("expected record duplicate exception");
      } catch (ORecordDuplicatedException ex) {

      }
      try {
        db.command(
            new OCommandScript(
                "sql",
                "BEGIN \n LET a = create vertex Test content {\"id\": \"12345678\"} \n LET b = create vertex Test content {\"id\": \"4kkrPhGe\"} \n LET c =create vertex Test content { \"id\": \"4kkrPhGe\"} \n RETURN $b \n COMMIT"))
            .execute();
        Assert.fail("expected record duplicate exception");
      } catch (ORecordDuplicatedException ex) {

      }

      List<ODocument> res = db.query(new OSQLSynchQuery("select from Test"));
      Assert.assertEquals(0, res.size());
    } finally {
      db.drop();
    }
  }

  @Test
  public void testDuplicateAlreadyExistingRollback() {
    ODatabaseDocument db = new ODatabaseDocumentTx("memory:" + TestGraphTransactionOnBatch.class.getSimpleName());
    try {
      db.create();
      db.getMetadata().getSchema().createClass("E");
      OClass V = db.getMetadata().getSchema().createClass("V");
      OClass clazz = db.getMetadata().getSchema().createClass("Test");
      clazz.setSuperClass(V);
      clazz.createProperty("id", OType.STRING).createIndex(INDEX_TYPE.UNIQUE);
      db.command(new OCommandSQL("create vertex Test SET id = \"12345678\"")).execute();
      try {
        db.command(new OCommandScript("sql", "BEGIN \n LET a = create vertex Test SET id = \"12345678\" \n RETURN $a \n COMMIT"))
            .execute();
        Assert.fail("expected record duplicate exception");
      } catch (ORecordDuplicatedException ex) {

      }
      List<ODocument> res = db.query(new OSQLSynchQuery("select from Test"));
      Assert.assertEquals(1, res.size());
    } finally {
      db.drop();
    }
  }

  @Test
  public void testDuplicateEdgeRollback() {
    ODatabaseDocument db = new ODatabaseDocumentTx("memory:" + TestGraphTransactionOnBatch.class.getSimpleName());
    try {
      db.create();
      OClass E = db.getMetadata().getSchema().createClass("E");
      db.getMetadata().getSchema().createClass("V");
      OClass clazz = db.getMetadata().getSchema().createClass("Test");
      clazz.setSuperClass(E);
      clazz.createProperty("aKey", OType.STRING).createIndex(INDEX_TYPE.UNIQUE);
      try {
        db.command(
            new OCommandScript(
                "sql",
                "BEGIN \n LET a = create vertex V \n LET b = create vertex V \n LET c =create edge Test from $a to $b SET aKey = \"12345\" \n LET d =create edge Test from $a to $b SET aKey = \"12345\"  \n RETURN $c \n COMMIT"))
            .execute();
        Assert.fail("expected record duplicate exception");
      } catch (ORecordDuplicatedException ex) {

      }
      List<ODocument> res = db.query(new OSQLSynchQuery("select from Test"));
      Assert.assertEquals(0, res.size());
    } finally {
      db.drop();
    }
  }

  @Test
  public void testDuplicateEdgeAlreadyPresentRollback() {
    ODatabaseDocument db = new ODatabaseDocumentTx("memory:" + TestGraphTransactionOnBatch.class.getSimpleName());
    try {
      db.create();
      OClass E = db.getMetadata().getSchema().createClass("E");
      db.getMetadata().getSchema().createClass("V");
      OClass clazz = db.getMetadata().getSchema().createClass("Test");
      clazz.setSuperClass(E);
      clazz.createProperty("aKey", OType.STRING).createIndex(INDEX_TYPE.UNIQUE);
      db.command(
          new OCommandScript(
              "sql",
              "BEGIN \n LET a = create vertex V \n LET b = create vertex V \n LET c =create edge Test from $a to $b SET aKey = \"12345\" \n RETURN $c  \n commit \n"))
          .execute();
      try {
        db.command(
            new OCommandScript(
                "sql",
                "BEGIN \n LET a = create vertex V \n LET b = create vertex V \n LET c =create edge Test from $a to $b SET aKey = \"12345\"\n RETURN $c \n COMMIT"))
            .execute();
        Assert.fail("expected record duplicate exception");
      } catch (ORecordDuplicatedException ex) {

      }
      List<ODocument> res = db.query(new OSQLSynchQuery("select from Test"));
      Assert.assertEquals(1, res.size());
    } finally {
      db.drop();
    }
  }

  @Test
  public void testReferInTxDeleteVertex() {

    ODatabaseDocument db = new ODatabaseDocumentTx("memory:" + TestGraphTransactionOnBatch.class.getSimpleName());
    try {
      db.create();
      db.getMetadata().getSchema().createClass("E");
      OClass V = db.getMetadata().getSchema().createClass("V");
      try {
        db.command(new OCommandSQL("create vertex V set Mid = '1' ")).execute();
        db.command(
            new OCommandScript("sql", "begin \n LET t0 = select from V where Mid='1' \n"
                + "LET t1 = delete vertex V where Mid = '1' \n LET t2 = create vertex V set Mid = '2' \n"
                + "LET t4 = create edge E from $t2 to $t0 \n commit \n return [$t4] ")).execute();
        Assert.fail("it should go in exception because referring to a in transaction delete vertex");
      } catch (Exception ex) {
      }
      List<ODocument> res = db.query(new OSQLSynchQuery("select from E"));
      Assert.assertEquals(res.size(), 0);
    } finally {
      db.drop();
    }
  }

  // @Test disabled because failing
  public void testReferToInTxCreatedAndDeletedVertex() {

    ODatabaseDocument db = new ODatabaseDocumentTx("memory:" + TestGraphTransactionOnBatch.class.getSimpleName());
    try {
      db.create();
      db.getMetadata().getSchema().createClass("E");
      OClass V = db.getMetadata().getSchema().createClass("V");
      try {
        db.command(
            new OCommandScript("sql", "begin \n LET t0 = create vertex V set Mid = '1' \n"
                + "LET t1 = delete vertex V where Mid = '1' \n LET t2 = create vertex V set Mid = '2' \n"
                + "LET t4 = create edge E from $t2 to $t0 \n commit \n return [$t4] ")).execute();
        Assert.fail("it should go in exception because referring to a in transaction delete vertex");
      } catch (Exception ex) {
      }
      List<ODocument> res = db.query(new OSQLSynchQuery("select from E"));
      Assert.assertEquals(res.size(), 0);
    } finally {
      db.drop();
    }
  }

  // @Test disabled because failing
  public void testReferToNotExistingVertex() {

    ODatabaseDocument db = new ODatabaseDocumentTx("memory:" + TestGraphTransactionOnBatch.class.getSimpleName());
    try {
      db.create();
      db.getMetadata().getSchema().createClass("E");
      OClass V = db.getMetadata().getSchema().createClass("V");
      try {
        db.command(
            new OCommandScript("sql", "begin \n \n LET t2 = create vertex V set Mid = \"2\" \n"
                + "LET t5 = select from V where Mid = '123456789' \n LET t3 = create edge E from $t5 to $t2 \n"
                + "\n commit \n return [$t3] ")).execute();
        Assert.fail("it should go in exception because referring to a not existing vertex");
      } catch (Exception ex) {
      }
      List<ODocument> res = db.query(new OSQLSynchQuery("select from E"));
      Assert.assertEquals(res.size(), 0);
    } finally {
      db.drop();
    }
  }

  @Test
  public void testReferToNotExistingVariableInTx() {
    ODatabaseDocument db = new ODatabaseDocumentTx("memory:" + TestGraphTransactionOnBatch.class.getSimpleName());
    try {
      db.create();
      db.getMetadata().getSchema().createClass("E");
      db.getMetadata().getSchema().createClass("V");
      db.command(new OCommandSQL(" create vertex V set Mid ='2'")).execute();
      List<ODocument> res = db.query(new OSQLSynchQuery("select from V"));
      Assert.assertEquals(1, res.size());

      try {
        db.command(
            new OCommandScript("sql",
                "begin \n Let t0 = delete vertex V where Mid='2' \n LET t1 = create edge E from $t2 to $t3 \n commit \n return $t1 "))
            .execute();
        Assert.fail("it should go in exception because referring to not existing variable");
      } catch (Exception ex) {
      }
      Assert.assertFalse(db.getTransaction().isActive());

      res = db.query(new OSQLSynchQuery("select from V"));
      Assert.assertEquals(1, res.size());
    } finally {
      db.drop();
    }
  }

}
