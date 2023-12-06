package com.orientechnologies.orient.core.sql;

import static org.junit.Assert.assertEquals;

import com.orientechnologies.BaseMemoryDatabase;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import java.util.List;
import org.junit.Test;

public class UpdateWithRidParameters extends BaseMemoryDatabase {

  @Test
  public void testRidParameters() {

    OSchema schm = db.getMetadata().getSchema();
    schm.createClass("testingClass");
    schm.createClass("testingClass2");

    db.command("INSERT INTO testingClass SET id = ?", 123).close();

    db.command("INSERT INTO testingClass2 SET id = ?", 456).close();
    ORID orid;
    try (OResultSet docs = db.query("SELECT FROM testingClass2 WHERE id = ?", 456)) {
      orid = (ORID) docs.next().getProperty("@rid");
    }

    // This does not work. It silently adds a null instead of the ORID.
    db.command("UPDATE testingClass set linkedlist = linkedlist || ?", orid).close();

    // This does work.
    db.command("UPDATE testingClass set linkedlist = linkedlist || " + orid.toString()).close();
    List<ORID> lst;
    try (OResultSet docs = db.query("SELECT FROM testingClass WHERE id = ?", 123)) {
      lst = docs.next().getProperty("linkedlist");
    }

    assertEquals(orid, lst.get(0));
    assertEquals(orid, lst.get(1));
  }
}
