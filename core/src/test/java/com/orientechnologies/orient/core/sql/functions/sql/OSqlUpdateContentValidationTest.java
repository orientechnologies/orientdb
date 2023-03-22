package com.orientechnologies.orient.core.sql.functions.sql;

import com.orientechnologies.BaseMemoryDatabase;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.exception.OValidationException;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import org.junit.Assert;
import org.junit.Test;

public class OSqlUpdateContentValidationTest extends BaseMemoryDatabase {

  @Test
  public void testReadOnlyValidation() {
    OClass clazz = db.getMetadata().getSchema().createClass("Test");
    clazz.createProperty("testNormal", OType.STRING);
    clazz.createProperty("test", OType.STRING).setReadonly(true);

    OIdentifiable res =
        db.command(
                new OCommandSQL(
                    "insert into Test content {\"testNormal\":\"hello\",\"test\":\"only read\"} "))
            .execute();
    try {
      db.command("update " + res.getIdentity() + " CONTENT {\"testNormal\":\"by\"}").close();
      Assert.fail("Error on update of a record removing a readonly property");
    } catch (OValidationException val) {

    }
  }
}
