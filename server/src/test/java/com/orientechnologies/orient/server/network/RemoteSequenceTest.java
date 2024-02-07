package com.orientechnologies.orient.server.network;

import static org.junit.Assert.assertNotEquals;

import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.server.BaseServerMemoryDatabase;
import org.junit.Test;

/** Created by tglman on 19/07/17. */
public class RemoteSequenceTest extends BaseServerMemoryDatabase {

  @Test
  public void testSequences() {
    ODatabaseDocument database = db;
    database.command("CREATE CLASS SV extends V").close();
    database.command("CREATE SEQUENCE seqCounter TYPE ORDERED").close();
    database.command("CREATE PROPERTY SV.uniqueID Long").close();
    database.command("CREATE PROPERTY SV.testID Long").close();
    database.command("ALTER PROPERTY SV.uniqueID NOTNULL true").close();
    database.command("ALTER PROPERTY SV.uniqueID MANDATORY true").close();
    database.command("ALTER PROPERTY SV.uniqueID READONLY true").close();
    database
        .command("ALTER PROPERTY SV.uniqueID DEFAULT 'sequence(\"seqCounter\").next()'")
        .close();
    database.command("CREATE CLASS CV1 extends SV").close();
    database.command("CREATE CLASS CV2 extends SV").close();
    database.command("CREATE INDEX uniqueID ON SV (uniqueID) UNIQUE").close();
    database.command("CREATE INDEX testid ON SV (testID) UNIQUE").close();
    database.reload();

    database.begin();
    ODocument doc = new ODocument("CV1");
    doc.field("testID", 1);
    database.save(doc);
    ODocument doc1 = new ODocument("CV1");
    doc1.field("testID", 1);
    database.save(doc1);
    assertNotEquals(doc1.field("uniqueID"), doc.field("uniqueID"));
  }
}
