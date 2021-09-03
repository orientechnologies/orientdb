package com.orientechnologies.orient.core.sql.functions.sql;

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.exception.OValidationException;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import org.junit.Assert;
import org.junit.Test;

public class OSqlUpdateContentValidationTest {

  @Test
  public void testReadOnlyValidation() {
    ODatabaseDocumentTx db =
        new ODatabaseDocumentTx("memory:" + OSqlUpdateContentValidationTest.class.getSimpleName());
    db.create();
    OClass clazz = db.getMetadata().getSchema().createClass("Test");
    clazz.createProperty("testNormal", OType.STRING);
    clazz.createProperty("test", OType.STRING).setReadonly(true);

    OIdentifiable res =
        db.command(
                new OCommandSQL(
                    "insert into Test content {\"testNormal\":\"hello\",\"test\":\"only read\"} "))
            .execute();
    try {
      db.command(
              new OCommandSQL("update " + res.getIdentity() + " CONTENT {\"testNormal\":\"by\"}"))
          .execute();
      Assert.fail("Error on update of a record removing a readonly property");
    } catch (OValidationException val) {

    }
    db.close();
  }
}
