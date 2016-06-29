package com.orientechnologies.orient.core.sql.functions.text;

import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import org.junit.Test;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class OSQLFunctionConcatTest {

  @Test
  public void testConcat() {
    ODatabaseDocument db = new ODatabaseDocumentTx("memory:testConCat");
    try {
      db.create();
      OClass concat = db.getMetadata().getSchema().createClass("ConCat");
      concat.createProperty("name", OType.STRING);

      ODocument doc;
      doc = new ODocument(concat);
      doc.field("name", "a");
      db.save(doc);
      doc = new ODocument(concat);
      doc.field("name", "b");
      db.save(doc);
      doc = new ODocument(concat);
      doc.field("name", "c");
      db.save(doc);

      List<ODocument> results = db.query(new OSQLSynchQuery<ODocument>("select concat(name) from ConCat order by name"));
      assertNotNull(results);
      assertEquals(results.size(), 1);
      assertEquals(results.get(0).field("concat"), "abc");

      results = db.query(new OSQLSynchQuery<ODocument>("select concat(name, ', ') from ConCat order by name"));
      assertNotNull(results);
      assertEquals(results.size(), 1);
      assertEquals(results.get(0).field("concat"), "a, b, c");
    } finally {
      db.drop();
    }
  }
}
