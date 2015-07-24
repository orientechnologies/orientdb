package com.orientechnologies.orient.core.index;

import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import org.testng.Assert;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.sql.OCommandSQL;

import java.util.List;

public class OCompositeIndexSQLInsertTest {

  public ODatabaseDocument db;

  @BeforeTest
  public void before() {
    db = new ODatabaseDocumentTx("memory:" + OCompositeIndexSQLInsertTest.class.getSimpleName());
    db.create();
    OSchema schema = db.getMetadata().getSchema();
    OClass book = schema.createClass("Book");
    book.createProperty("author", OType.STRING);
    book.createProperty("title", OType.STRING);
    book.createProperty("publicationYears", OType.EMBEDDEDLIST, OType.INTEGER);
    book.createIndex("books", "unique", "author", "title", "publicationYears");
  }

  @Test
  public void testIndexInsert() {
    db.command(
        new OCommandSQL(
            "insert into index:books (key, rid) values ([\"Donald Knuth\", \"The Art of Computer Programming\", 1968], #12:0)"))
        .execute();

  }

  @Test(expectedExceptions = OException.class)
  public void testIndexInsertNull() {
    db.command(new OCommandSQL("insert into index:books (key, rid) values (null, #12:0)")).execute();
  }

  @Test()
  public void testIndexOfStrings() {
    db.command(new OCommandSQL("CREATE INDEX test unique string,string")).execute();
    db.command(new OCommandSQL("insert into index:test (key, rid) values (['a','b'], #12:0)")).execute();
  }

  @Test
  public void testCompositeIndexWithRangeAndContains() {
    ODatabaseDocument database = db;

    final OSchema schema = database.getMetadata().getSchema();
    OClass clazz = schema.createClass("CompositeIndexWithRangeAndConditions");
    clazz.createProperty("id", OType.INTEGER);
    clazz.createProperty("bar", OType.INTEGER);
    clazz.createProperty("tags", OType.EMBEDDEDLIST, OType.STRING);
    clazz.createProperty("name", OType.STRING);

    database
        .command(
            new OCommandSQL(
                "create index CompositeIndexWithRangeAndConditions_id_tags_name on CompositeIndexWithRangeAndConditions (id, tags, name) NOTUNIQUE"))
        .execute();

    database
        .command(
            new OCommandSQL(
                "insert into CompositeIndexWithRangeAndConditions set id = 1, tags = [\"green\",\"yellow\"] , name = \"Foo\", bar = 1"))
        .execute();
    database.command(
        new OCommandSQL(
            "insert into CompositeIndexWithRangeAndConditions set id = 1, tags = [\"blue\",\"black\"] , name = \"Foo\", bar = 14"))
        .execute();
    database.command(
        new OCommandSQL("insert into CompositeIndexWithRangeAndConditions set id = 1, tags = [\"white\"] , name = \"Foo\""))
        .execute();
    database
        .command(
            new OCommandSQL(
                "insert into CompositeIndexWithRangeAndConditions set id = 1, tags = [\"green\",\"yellow\"], name = \"Foo1\", bar = 14"))
        .execute();

    List<ODocument> r = database.query(new OSQLSynchQuery<Object>(
        "select from CompositeIndexWithRangeAndConditions where id > 0 and bar = 1"));
    Assert.assertEquals(1, r.size());

    List<ODocument> r1 = database.query(new OSQLSynchQuery<Object>(
        "select from CompositeIndexWithRangeAndConditions where id = 1 and tags CONTAINS \"white\""));

    List<ODocument> r2 = database.query(new OSQLSynchQuery<Object>(
        "select from CompositeIndexWithRangeAndConditions where id > 0 and tags CONTAINS \"white\""));

    List<ODocument> r3 = database.query(new OSQLSynchQuery<Object>(
        "select from CompositeIndexWithRangeAndConditions where id > 0 and bar = 1"));

    Assert.assertEquals(r1.size(), 1);
    Assert.assertEquals(r2.size(), 1);
    Assert.assertEquals(r3.size(), 1);

    List<ODocument> r4 = database.query(new OSQLSynchQuery<Object>(
        "select from CompositeIndexWithRangeAndConditions where tags CONTAINS \"white\" and id > 0"));
    Assert.assertEquals(r4.size(), 1);
  }

  @AfterTest
  public void after() {
    db.drop();
  }

}
