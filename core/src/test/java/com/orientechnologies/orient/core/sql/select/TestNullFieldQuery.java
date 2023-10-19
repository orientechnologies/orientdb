package com.orientechnologies.orient.core.sql.select;

import static org.junit.Assert.assertTrue;

import com.orientechnologies.BaseMemoryDatabase;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import java.util.List;
import org.junit.Test;

/** Created by tglman on 21/07/16. */
public class TestNullFieldQuery extends BaseMemoryDatabase {

  @Test
  public void testQueryNullValue() {
    db.getMetadata().getSchema().createClass("Test");
    ODocument doc = new ODocument("Test");
    doc.field("name", (Object) null);
    db.save(doc);

    List<ODocument> res =
        db.query(new OSQLSynchQuery<ODocument>("select from Test where name= 'some' "));
    assertTrue(res.isEmpty());
  }

  @Test
  public void testQueryNullValueSchemaFull() {
    OClass clazz = db.getMetadata().getSchema().createClass("Test");
    clazz.createProperty("name", OType.ANY);
    ODocument doc = new ODocument("Test");
    doc.field("name", (Object) null);
    db.save(doc);

    List<ODocument> res =
        db.query(new OSQLSynchQuery<ODocument>("select from Test where name= 'some' "));
    assertTrue(res.isEmpty());
  }
}
