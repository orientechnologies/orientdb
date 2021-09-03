package com.orientechnologies.orient.test.internal;

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OClass.INDEX_TYPE;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import java.io.IOException;
import java.util.List;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class TestOrientPagination {
  public static final String dbName = "memory:/testDB";
  public static final String dbUser = "admin";
  public static final String dbPwd = "admin";

  private static ODatabaseDocumentTx database = null;

  @BeforeClass
  public static void init() throws IOException {
    // database = new ODatabaseDocumentTx(dbName);
    // ODatabaseHelper.createDatabase(database, dbName);

    database = new ODatabaseDocumentTx(dbName);
    database.open(dbUser, dbPwd);

    OSchema schema = database.getMetadata().getSchema();

    schema.createClass("test_class");
    OClass oclass = schema.getClass("test_class");
    oclass.createProperty("testp", OType.LONG);
    oclass.createIndex("testp_idx", INDEX_TYPE.NOTUNIQUE, "testp");

    ODocument doc = new ODocument(oclass);
    doc.field("testp", 1);

    ODocument doc1 = new ODocument(oclass);
    doc1.field("testp", 2);

    ODocument doc2 = new ODocument(oclass);
    doc2.field("testp", 3);

    ODocument doc3 = new ODocument(oclass);
    doc3.field("testp", 4);

    ODocument doc4 = new ODocument(oclass);
    doc4.field("testp", 5);

    ODocument doc5 = new ODocument(oclass);
    doc5.field("testp", 6);

    ODocument doc6 = new ODocument(oclass);
    doc6.field("testp", 7);

    database.save(doc);
    database.save(doc1);
    database.save(doc2);
    database.save(doc3);
    database.save(doc4);
    database.save(doc5);
    database.save(doc6);
  }

  @Test
  public void testOrderBySkip() {
    String query = "select from test_class order by testp limit 3 skip ";
    List<ODocument> docs = database.query(new OSQLSynchQuery<ODocument>(query + "0"));
    List<ODocument> docs2 = database.query(new OSQLSynchQuery<ODocument>(query + "3"));

    // Assert.assertFalse(docs.equals(docs2));
  }

  @Test
  public void testOrderByDescSkip() {
    String query = "select from test_class order by testp desc limit 3 skip ";
    // List<ODocument> docs = database.query(new
    // OSQLSynchQuery<ODocument>(query + "0"));
    List<ODocument> docs2 = database.query(new OSQLSynchQuery<ODocument>(query + "3"));
    // Assert.assertFalse(docs.equals(docs2));
  }
}
