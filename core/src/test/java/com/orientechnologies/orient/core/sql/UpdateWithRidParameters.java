package com.orientechnologies.orient.core.sql;

import static org.junit.Assert.assertEquals;

import com.orientechnologies.BaseMemoryDatabase;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
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

    List<ODocument> docs =
        db.query(new OSQLSynchQuery<ODocument>("SELECT FROM testingClass2 WHERE id = ?"), 456);
    ORID orid = (ORID) docs.get(0).field("@rid", ORID.class);

    // This does not work. It silently adds a null instead of the ORID.
    db.command("UPDATE testingClass set linkedlist = linkedlist || ?", orid).close();

    // This does work.
    db.command("UPDATE testingClass set linkedlist = linkedlist || " + orid.toString()).close();

    List<ODocument> docs2 =
        db.query(new OSQLSynchQuery<ODocument>("SELECT FROM testingClass WHERE id = ?"), 123);
    List<ORID> lst = docs2.get(0).field("linkedlist", List.class);

    assertEquals(orid, lst.get(0));
    assertEquals(orid, lst.get(1));
  }
}
