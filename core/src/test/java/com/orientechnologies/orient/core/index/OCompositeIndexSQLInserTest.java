package com.orientechnologies.orient.core.index;

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

public class OCompositeIndexSQLInserTest {

  public ODatabaseDocument db;

  @BeforeTest
  public void before() {
    db = new ODatabaseDocumentTx("memory:" + OCompositeIndexSQLInserTest.class.getSimpleName());
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

  @AfterTest
  public void after() {
    db.drop();
  }

}
