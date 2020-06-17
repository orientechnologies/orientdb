package com.orientechnologies.orient.core.sql;

import static org.junit.Assert.assertEquals;

import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import java.util.List;
import org.junit.Test;

public class UpdateWithRidParameters {

  @Test
  public void testRidParameters() {
    ODatabaseDocument db =
        new ODatabaseDocumentTx("memory:" + UpdateWithRidParameters.class.getSimpleName());
    db.create();

    OSchema schm = db.getMetadata().getSchema();
    schm.createClass("testingClass");
    schm.createClass("testingClass2");

    OCommandSQL cmd = new OCommandSQL("INSERT INTO testingClass SET id = ?");
    db.command(cmd).execute(123);

    OCommandSQL cmd2 = new OCommandSQL("INSERT INTO testingClass2 SET id = ?");
    db.command(cmd2).execute(456);

    List<ODocument> docs =
        db.query(new OSQLSynchQuery<ODocument>("SELECT FROM testingClass2 WHERE id = ?"), 456);
    ORID orid = (ORID) docs.get(0).field("@rid", ORID.class);

    // This does not work. It silently adds a null instead of the ORID.
    OCommandSQL cmd3 = new OCommandSQL("UPDATE testingClass ADD linkedlist = ?");
    db.command(cmd3).execute(orid);

    // This does work.
    OCommandSQL cmd4 = new OCommandSQL("UPDATE testingClass ADD linkedlist = " + orid.toString());
    db.command(cmd4).execute();

    List<ODocument> docs2 =
        db.query(new OSQLSynchQuery<ODocument>("SELECT FROM testingClass WHERE id = ?"), 123);
    List<ORID> lst = docs2.get(0).field("linkedlist", List.class);

    assertEquals(orid, lst.get(0));
    assertEquals(orid, lst.get(1));

    db.drop();
  }
}
