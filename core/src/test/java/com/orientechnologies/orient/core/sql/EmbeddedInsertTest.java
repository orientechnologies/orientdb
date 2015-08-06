package com.orientechnologies.orient.core.sql;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.record.impl.ODocument;

public class EmbeddedInsertTest {

  @Test
  public void testEmbeddedValueDate() {
    ODatabaseDocument db = new ODatabaseDocumentTx("memory:" + EmbeddedInsertTest.class.getSimpleName());
    db.create();

    try {
      OSchema schm = db.getMetadata().getSchema();
      schm.createClass("myClass");

      final ODocument res = db
          .command(
              new OCommandSQL(
                  "insert into myClass (name,meta) values (\"claudio\",{\"@type\":\"d\",\"country\":\"italy\", \"date\":\"2013-01-01\",\"@fieldTypes\":\"date=a\"}) return @this"))
          .execute();

      final ODocument embedded = res.field("meta");
      Assert.assertNotNull(embedded);

      Assert.assertEquals(embedded.fields(), 2);
      Assert.assertEquals(embedded.field("country"), "italy");
      Assert.assertEquals(embedded.field("date").getClass(), java.util.Date.class);
    } finally {
      db.drop();
    }
  }
}
