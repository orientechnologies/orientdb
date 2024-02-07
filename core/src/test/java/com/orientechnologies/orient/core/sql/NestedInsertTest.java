package com.orientechnologies.orient.core.sql;

import com.orientechnologies.BaseMemoryDatabase;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import org.junit.Assert;
import org.junit.Test;

public class NestedInsertTest extends BaseMemoryDatabase {

  @Test
  public void testEmbeddedValueDate() {
    OSchema schm = db.getMetadata().getSchema();
    schm.createClass("myClass");

    OResultSet result =
        db.command(
            "insert into myClass (name,meta) values"
                + " (\"claudio\",{\"@type\":\"d\",\"country\":\"italy\","
                + " \"date\":\"2013-01-01\",\"@fieldTypes\":\"date=a\"}) return @this");
    final ODocument res = ((OIdentifiable) result.next().getProperty("@this")).getRecord();
    final ODocument embedded = res.field("meta");
    Assert.assertNotNull(embedded);

    Assert.assertEquals(embedded.fields(), 2);
    Assert.assertEquals(embedded.field("country"), "italy");
    Assert.assertEquals(embedded.field("date").getClass(), java.util.Date.class);
  }

  @Test
  public void testLinkedNested() {
    OSchema schm = db.getMetadata().getSchema();
    OClass cl = schm.createClass("myClass");
    OClass linked = schm.createClass("Linked");
    cl.createProperty("some", OType.LINK, linked);

    OResultSet result =
        db.command(
            "insert into myClass set some ={\"@type\":\"d\",\"@class\":\"Linked\",\"name\":\"a"
                + " name\"} return @this");

    final ODocument res = ((OIdentifiable) result.next().getProperty("@this")).getRecord();
    final ODocument ln = res.field("some");
    Assert.assertNotNull(ln);
    Assert.assertTrue(ln.getIdentity().isPersistent());
    Assert.assertEquals(ln.fields(), 1);
    Assert.assertEquals(ln.field("name"), "a name");
  }
}
