package com.orientechnologies.orient.graph;

import com.orientechnologies.orient.core.command.script.OCommandScript;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OClass.INDEX_TYPE;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import com.orientechnologies.orient.core.storage.ORecordDuplicatedException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

public class TestGraphTransactionOnBatch {
  private ODatabaseDocument db;
  private OClass            V;
  private OClass            E;

  @Before
  public void before() {
    db = new ODatabaseDocumentTx("memory:" + TestGraphTransactionOnBatch.class.getSimpleName());
    db.create();
    V = db.getMetadata().getSchema().createClass("V");
    E = db.getMetadata().getSchema().createClass("E");
  }

  @After
  public void after() {
    db.activateOnCurrentThread();
    db.drop();
  }

  @Test
  public void testDuplicateRollback() {
    OClass clazz = db.getMetadata().getSchema().createClass("Test");
    clazz.setSuperClass(V);
    clazz.createProperty("id", OType.STRING).createIndex(INDEX_TYPE.UNIQUE);
    try {
      db.command(
          new OCommandScript(
              "sql",
              "BEGIN \n LET a = create vertex Test SET id = \"12345678\" \n LET b = create vertex Test SET id = \"4kkrPhGe\" \n LET c =create vertex Test SET id = \"4kkrPhGe\" \n COMMIT \n RETURN $b "))
          .execute();
      Assert.fail("expected record duplicate exception");
    } catch (ORecordDuplicatedException ex) {

    }
    try {
      db.command(
          new OCommandScript(
              "sql",
              "BEGIN \n LET a = create vertex Test content {\"id\": \"12345678\"} \n LET b = create vertex Test content {\"id\": \"4kkrPhGe\"} \n LET c =create vertex Test content { \"id\": \"4kkrPhGe\"} \n COMMIT \n RETURN $b "))
          .execute();
      Assert.fail("expected record duplicate exception");
    } catch (ORecordDuplicatedException ex) {

    }

    List<ODocument> res = db.query(new OSQLSynchQuery("select from Test"));
    Assert.assertEquals(0, res.size());
  }

  @Test
  public void testDuplicateAlreadyExistingRollback() {
    OClass clazz = db.getMetadata().getSchema().createClass("Test");
    clazz.setSuperClass(V);
    clazz.createProperty("id", OType.STRING).createIndex(INDEX_TYPE.UNIQUE);
    db.command(new OCommandSQL("create vertex Test SET id = \"12345678\"")).execute();
    try {
      db.command(new OCommandScript("sql", "BEGIN \n LET a = create vertex Test SET id = \"12345678\" \n COMMIT\n"
          + " RETURN $a"))
          .execute();
      Assert.fail("expected record duplicate exception");
    } catch (ORecordDuplicatedException ex) {

    }
    List<ODocument> res = db.query(new OSQLSynchQuery("select from Test"));
    Assert.assertEquals(1, res.size());
  }

  @Test
  public void testDuplicateEdgeRollback() {
    OClass clazz = db.getMetadata().getSchema().createClass("Test");
    clazz.setSuperClass(E);
    clazz.createProperty("aKey", OType.STRING).createIndex(INDEX_TYPE.UNIQUE);
    try {
      db.command(
          new OCommandScript(
              "sql",
              "BEGIN \n LET a = create vertex V \n LET b = create vertex V \n LET c =create edge Test from $a to $b SET aKey = \"12345\" \n LET d =create edge Test from $a to $b SET aKey = \"12345\"  \n COMMIT \n"
                  + " RETURN $c"))
          .execute();
      Assert.fail("expected record duplicate exception");
    } catch (ORecordDuplicatedException ex) {

    }
    List<ODocument> res = db.query(new OSQLSynchQuery("select from Test"));
    Assert.assertEquals(0, res.size());
  }

  @Test
  public void testAbsoluteDuplicateEdgeRollback() {
    OClass clazz = db.getMetadata().getSchema().createClass("Test");
    clazz.setSuperClass(E);
    clazz.createProperty("in", OType.LINK);
    clazz.createProperty("out", OType.LINK);
    clazz.createIndex("Unique", INDEX_TYPE.UNIQUE, "in", "out");
    try {
      db.command(
          new OCommandScript(
              "sql",
              "BEGIN \n LET a = create vertex V \n LET b = create vertex V \n LET c =create edge Test from $a to $b  \n LET d =create edge Test from $a to $b  \n COMMIT \n"
                  + " RETURN $c"))
          .execute();
      Assert.fail("expected record duplicate exception");
    } catch (ORecordDuplicatedException ex) {

    }
    List<ODocument> res = db.query(new OSQLSynchQuery("select from Test"));
    Assert.assertEquals(0, res.size());
  }

  @Test
  public void testAbsoluteDuplicateEdgeAlreadyPresentRollback() {
    OClass clazz = db.getMetadata().getSchema().createClass("Test");
    clazz.setSuperClass(E);
    clazz.createProperty("in", OType.LINK);
    clazz.createProperty("out", OType.LINK);
    clazz.createIndex("Unique", INDEX_TYPE.UNIQUE, "in", "out");
    try {
      db.command(
          new OCommandScript(
              "sql",
              "BEGIN \n LET a = create vertex V set name='a' \n LET b = create vertex V  set name='b' \n LET c =create edge Test from $a to $b  \n LET d =create edge Test from $a to $b \n COMMIT \n"
                  + " RETURN $c"))
          .execute();

      db.command(
          new OCommandScript("sql",
              "BEGIN \n LET c =create edge Test from (select from V  where name='a') to (select from V where name='b')  \n COMMIT \n"
                  + " RETURN $c"))
          .execute();
      Assert.fail("expected record duplicate exception");
    } catch (ORecordDuplicatedException ex) {

    }
    List<ODocument> res = db.query(new OSQLSynchQuery("select from Test"));
    Assert.assertEquals(0, res.size());
  }

  @Test
  public void testDuplicateEdgeAlreadyPresentRollback() {
    OClass clazz = db.getMetadata().getSchema().createClass("Test");
    clazz.setSuperClass(E);
    clazz.createProperty("aKey", OType.STRING).createIndex(INDEX_TYPE.UNIQUE);
    db.command(
        new OCommandScript(
            "sql",
            "BEGIN \n LET a = create vertex V \n LET b = create vertex V \n LET c =create edge Test from $a to $b SET aKey = \"12345\"  \n commit  \n"
                + " RETURN $c"))
        .execute();
    try {
      db.command(
          new OCommandScript(
              "sql",
              "BEGIN \n LET a = create vertex V \n LET b = create vertex V \n LET c =create edge Test from $a to $b SET aKey = \"12345\"\n COMMIT \n"
                  + " RETURN $c"))
          .execute();
      Assert.fail("expected record duplicate exception");
    } catch (ORecordDuplicatedException ex) {

    }
    List<ODocument> res = db.query(new OSQLSynchQuery("select from Test"));
    Assert.assertEquals(1, res.size());
  }

  @Test
  public void testReferInTxDeleteVertex() {
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
  }

  @Test
  public void testReferToInTxCreatedAndDeletedVertex() {

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
  }

  /**
   * This test is different from the original reported, because in case of empty query result the 'create edge ' command just don't
   * create edges without failing
   *
   */
  @Test
  public void testReferToNotExistingVertex() {
    try {
      db.command(
          new OCommandScript("sql", "begin \n \n LET t2 = create vertex V set Mid = \"2\" \n"
              + "LET t5 = select from V where Mid = '123456789' \n LET t3 = create edge E from $t5 to $t2 \n"
              + "\n commit \n return [$t3] ")).execute();
      Assert.fail();
    } catch (OCommandExecutionException e) {
    }
    List<ODocument> res = db.query(new OSQLSynchQuery("select from E"));
    Assert.assertEquals(res.size(), 0);
  }

  @Test
  public void testReferToNotExistingVariableInTx() {
    db.command(new OCommandSQL(" create vertex V set Mid ='2'")).execute();
    Assert.assertFalse(db.getTransaction().isActive());

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
  }

}
