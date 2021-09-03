package com.orientechnologies.orient.server.network;

import static org.junit.Assert.assertNotEquals;

import com.orientechnologies.common.io.OFileUtils;
import com.orientechnologies.orient.client.remote.OServerAdmin;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.server.OServer;
import java.io.File;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/** Created by tglman on 19/07/17. */
public class RemoteSequenceTest {

  private OServer server;

  @Before
  public void before() throws Exception {
    server = new OServer(false);
    server.startup(getClass().getResourceAsStream("orientdb-server-config.xml"));
    server.activate();

    OServerAdmin server = new OServerAdmin("remote:localhost");
    server.connect("root", "root");
    server.createDatabase(RemoteSequenceTest.class.getSimpleName(), "graph", "memory");
  }

  @Test
  public void testSequences() {
    ODatabaseDocument database =
        new ODatabaseDocumentTx("remote:localhost/" + RemoteSequenceTest.class.getSimpleName());
    database.open("admin", "admin");
    database.command(new OCommandSQL("DROP CLASS CV1")).execute();
    database.command(new OCommandSQL("DROP CLASS CV2")).execute();
    database.command(new OCommandSQL("DROP CLASS SV")).execute();
    database.command(new OCommandSQL("DROP SEQUENCE seqCounter")).execute();
    database.command(new OCommandSQL("DROP index testID")).execute();
    database.command(new OCommandSQL("DROP index uniqueID")).execute();

    database.command(new OCommandSQL("CREATE CLASS SV extends V")).execute();
    database.command(new OCommandSQL("CREATE SEQUENCE seqCounter TYPE ORDERED")).execute();
    database.command(new OCommandSQL("CREATE PROPERTY SV.uniqueID Long")).execute();
    database.command(new OCommandSQL("CREATE PROPERTY SV.testID Long")).execute();
    database.command(new OCommandSQL("ALTER PROPERTY SV.uniqueID NOTNULL true")).execute();
    database.command(new OCommandSQL("ALTER PROPERTY SV.uniqueID MANDATORY true")).execute();
    database.command(new OCommandSQL("ALTER PROPERTY SV.uniqueID READONLY true")).execute();
    database
        .command(
            new OCommandSQL("ALTER PROPERTY SV.uniqueID DEFAULT 'sequence(\"seqCounter\").next()'"))
        .execute();
    database.command(new OCommandSQL("CREATE CLASS CV1 extends SV")).execute();
    database.command(new OCommandSQL("CREATE CLASS CV2 extends SV")).execute();
    database.command(new OCommandSQL("CREATE INDEX uniqueID ON SV (uniqueID) UNIQUE")).execute();
    database.command(new OCommandSQL("CREATE INDEX testid ON SV (testID) UNIQUE")).execute();
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

  @After
  public void after() {
    server.shutdown();

    Orient.instance().shutdown();
    OFileUtils.deleteRecursively(new File(server.getDatabaseDirectory()));
    Orient.instance().startup();
  }
}
