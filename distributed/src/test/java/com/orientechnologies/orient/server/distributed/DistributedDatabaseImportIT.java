package com.orientechnologies.orient.server.distributed;

import static org.junit.Assert.assertTrue;

import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.ODatabaseType;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.db.tool.ODatabaseExport;
import com.orientechnologies.orient.core.db.tool.ODatabaseImport;
import com.orientechnologies.orient.setup.LocalTestSetup;
import com.orientechnologies.orient.setup.SetupConfig;
import com.orientechnologies.orient.setup.configs.SimpleDServerConfig;
import java.io.File;
import java.io.IOException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class DistributedDatabaseImportIT {

  // Relies on direct access to OServer to import DB and can run only on local setup.
  private LocalTestSetup setup;
  private SetupConfig config;
  private String server0, server1, server2;

  @Before
  public void before() throws Exception {
    config = new SimpleDServerConfig();
    server0 = SimpleDServerConfig.SERVER0;
    server1 = SimpleDServerConfig.SERVER1;
    server2 = SimpleDServerConfig.SERVER2;
    setup = new LocalTestSetup(config);
    setup.setup();
  }

  @Test
  public void test() throws IOException {
    final OrientDB ctx1 = setup.getServer(server0).getServerInstance().getContext();
    ctx1.create("import-test", ODatabaseType.PLOCAL);
    final ODatabaseSession session = ctx1.open("import-test", "admin", "admin");
    session.createClass("testa");
    final ODatabaseExport export =
        new ODatabaseExport(
            (ODatabaseDocumentInternal) session, "target/export.tar.gz", iText -> {});
    export.exportDatabase();
    export.close();
    session.close();

    ctx1.create("imported-test", ODatabaseType.PLOCAL);
    final ODatabaseSession session1 = ctx1.open("imported-test", "admin", "admin");
    final ODatabaseImport imp =
        new ODatabaseImport(
            (ODatabaseDocumentInternal) session1, "target/export.tar.gz", iText -> {});
    imp.importDatabase();
    imp.close();
    session1.close();

    final OrientDB ctx2 = setup.getServer(server1).getServerInstance().getContext();
    final ODatabaseSession session2 = ctx2.open("imported-test", "admin", "admin");
    assertTrue(session2.getMetadata().getSchema().existsClass("testa"));
    session2.close();
  }

  @After
  public void after() throws InterruptedException {
    setup.getServer(server0).getServerInstance().dropDatabase("import-test");
    setup.getServer(server0).getServerInstance().dropDatabase("imported-test");
    setup.teardown();
    File file = new File("target/export.tar.gz");
    if (file.exists()) file.delete();
    ODatabaseDocumentTx.closeAll();
  }
}
