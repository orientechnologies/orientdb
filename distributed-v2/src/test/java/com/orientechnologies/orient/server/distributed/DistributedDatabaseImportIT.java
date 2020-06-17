package com.orientechnologies.orient.server.distributed;

import static org.junit.Assert.assertTrue;

import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.ODatabaseType;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.db.tool.ODatabaseExport;
import com.orientechnologies.orient.core.db.tool.ODatabaseImport;
import com.orientechnologies.orient.server.OServer;
import java.io.File;
import java.io.IOException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class DistributedDatabaseImportIT {

  private OServer server0;
  private OServer server1;
  private OServer server2;

  @Before
  public void before() throws Exception {
    server0 = OServer.startFromClasspathConfig("orientdb-simple-dserver-config-0.xml");
    server1 = OServer.startFromClasspathConfig("orientdb-simple-dserver-config-1.xml");
    server2 = OServer.startFromClasspathConfig("orientdb-simple-dserver-config-2.xml");
  }

  @Test
  public void test() throws IOException {
    OrientDB ctx1 = server0.getContext();
    ctx1.create("import-test", ODatabaseType.PLOCAL);
    ODatabaseSession session = ctx1.open("import-test", "admin", "admin");
    session.createClass("testa");
    ODatabaseExport export =
        new ODatabaseExport(
            (ODatabaseDocumentInternal) session, "target/export.tar.gz", iText -> {});
    export.exportDatabase();
    export.close();
    session.close();

    ctx1.create("imported-test", ODatabaseType.PLOCAL);
    ODatabaseSession session1 = ctx1.open("imported-test", "admin", "admin");
    ODatabaseImport imp =
        new ODatabaseImport(
            (ODatabaseDocumentInternal) session1, "target/export.tar.gz", iText -> {});
    imp.importDatabase();
    imp.close();
    session1.close();

    OrientDB ctx2 = server1.getContext();
    ODatabaseSession session2 = ctx2.open("imported-test", "admin", "admin");
    assertTrue(session2.getMetadata().getSchema().existsClass("testa"));
    session2.close();
  }

  @After
  public void after() throws InterruptedException {
    server0.dropDatabase("import-test");
    server0.dropDatabase("imported-test");
    server0.shutdown();
    server1.shutdown();
    server2.shutdown();
    File file = new File("target/export.tar.gz");
    if (file.exists()) file.delete();
    ODatabaseDocumentTx.closeAll();
  }
}
