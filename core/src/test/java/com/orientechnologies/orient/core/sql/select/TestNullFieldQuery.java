package com.orientechnologies.orient.core.sql.select;

import static org.junit.Assert.assertTrue;

import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import java.util.List;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/** Created by tglman on 21/07/16. */
public class TestNullFieldQuery {

  private ODatabaseDocument database;

  @Before
  public void before() {
    database = new ODatabaseDocumentTx("memory:" + TestNullFieldQuery.class.getSimpleName());
    database.create();
  }

  @After
  public void after() {
    database.drop();
  }

  @Test
  public void testQueryNullValue() {
    database.getMetadata().getSchema().createClass("Test");
    ODocument doc = new ODocument("Test");
    doc.field("name", (Object) null);
    database.save(doc);

    List<ODocument> res =
        database.query(new OSQLSynchQuery<ODocument>("select from Test where name= 'some' "));
    assertTrue(res.isEmpty());
  }

  @Test
  public void testQueryNullValueSchemaFull() {
    OClass clazz = database.getMetadata().getSchema().createClass("Test");
    clazz.createProperty("name", OType.ANY);
    ODocument doc = new ODocument("Test");
    doc.field("name", (Object) null);
    database.save(doc);

    List<ODocument> res =
        database.query(new OSQLSynchQuery<ODocument>("select from Test where name= 'some' "));
    assertTrue(res.isEmpty());
  }
}
