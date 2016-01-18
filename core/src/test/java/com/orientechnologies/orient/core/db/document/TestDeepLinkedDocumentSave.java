package com.orientechnologies.orient.core.db.document;

import com.orientechnologies.orient.core.record.impl.ODocument;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

public class TestDeepLinkedDocumentSave {

  @Test
  public void testLinked() {
    ODatabaseDocument db = new ODatabaseDocumentTx("memory:" + TestDeepLinkedDocumentSave.class.getSimpleName());
    db.create();
    try {
      db.getMetadata().getSchema().createClass("Test");
      ODocument doc = new ODocument("Test");
      for (int i = 0; i < 3000; i++)
        doc = new ODocument("Test").field("linked", doc);
      db.save(doc);

      assertEquals(3001, db.countClass("Test"));
    } finally {
      db.drop();
    }

  }

}
