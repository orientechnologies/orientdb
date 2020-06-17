package com.orientechnologies.orient.core.sql.orderby;

import static org.junit.Assert.assertEquals;

import com.orientechnologies.orient.core.db.ODatabase.ATTRIBUTES;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OClass.INDEX_TYPE;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import java.util.List;
import java.util.Locale;
import org.junit.Ignore;
import org.junit.Test;

public class TestOrderBy {

  @Test
  public void testGermanOrderBy() {
    ODatabaseDocument db = new ODatabaseDocumentTx("memory:testGermanOrderBy");
    db.set(ATTRIBUTES.LOCALECOUNTRY, Locale.GERMANY.getCountry());
    db.set(ATTRIBUTES.LOCALELANGUAGE, Locale.GERMANY.getLanguage());
    db.create();
    try {
      db.getMetadata().getSchema().createClass("test");
      ORecord res1 = db.save(new ODocument("test").field("name", "Ähhhh"));
      ORecord res2 = db.save(new ODocument("test").field("name", "Ahhhh"));
      ORecord res3 = db.save(new ODocument("test").field("name", "Zebra"));
      List<?> queryRes = db.query(new OSQLSynchQuery<Object>("select from test order by name"));
      assertEquals(queryRes.get(0), res2);
      assertEquals(queryRes.get(1), res1);
      assertEquals(queryRes.get(2), res3);

      queryRes = db.query(new OSQLSynchQuery<Object>("select from test order by name desc "));
      assertEquals(queryRes.get(0), res3);
      assertEquals(queryRes.get(1), res1);
      assertEquals(queryRes.get(2), res2);
    } finally {
      db.drop();
    }
  }

  @Test
  @Ignore
  public void testGermanOrderByIndex() {
    ODatabaseDocument db = new ODatabaseDocumentTx("memory:testGermanOrderBy");
    db.set(ATTRIBUTES.LOCALECOUNTRY, Locale.GERMANY.getCountry());
    db.set(ATTRIBUTES.LOCALELANGUAGE, Locale.GERMANY.getLanguage());
    db.create();
    try {
      OClass clazz = db.getMetadata().getSchema().createClass("test");
      clazz.createProperty("name", OType.STRING).createIndex(INDEX_TYPE.NOTUNIQUE);
      ORecord res1 = db.save(new ODocument("test").field("name", "Ähhhh"));
      ORecord res2 = db.save(new ODocument("test").field("name", "Ahhhh"));
      ORecord res3 = db.save(new ODocument("test").field("name", "Zebra"));
      List<?> queryRes = db.query(new OSQLSynchQuery<Object>("select from test order by name"));
      assertEquals(queryRes.get(0), res2);
      assertEquals(queryRes.get(1), res1);
      assertEquals(queryRes.get(2), res3);

      queryRes = db.query(new OSQLSynchQuery<Object>("select from test order by name desc "));
      assertEquals(queryRes.get(0), res3);
      assertEquals(queryRes.get(1), res1);
      assertEquals(queryRes.get(2), res2);
    } finally {
      db.drop();
    }
  }
}
