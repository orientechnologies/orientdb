package com.orientechnologies.orient.core.metadata.schema;

import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import org.testng.annotations.Test;

import static org.testng.AssertJUnit.assertEquals;

public class CreateClassMultipleClusterTest {

  @Test
  public void testCreateClassSQL() {
    ODatabaseDocument db = new ODatabaseDocumentTx("memory:" + CreateClassMultipleClusterTest.class.getSimpleName());
    db.create();
    try {
      db.command(new OCommandSQL("drop class V")).execute();
      db.command(new OCommandSQL("create class V clusters 16")).execute();
      db.command(new OCommandSQL("create class X extends V clusters 32")).execute();

      final OClass clazzV = db.getMetadata().getSchema().getClass("V");
      assertEquals(16, clazzV.getClusterIds().length);

      final OClass clazzX = db.getMetadata().getSchema().getClass("X");
      assertEquals(32, clazzX.getClusterIds().length);

    } finally {
      db.drop();
    }

  }
}
