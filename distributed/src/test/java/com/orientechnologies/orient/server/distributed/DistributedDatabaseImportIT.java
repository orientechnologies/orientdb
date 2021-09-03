package com.orientechnologies.orient.server.distributed;

import static org.junit.Assert.assertTrue;

import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.ODatabaseSession;
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
  private String exportFileName;

  @Before
  public void before() throws Exception {
    exportFileName = String.format("target/export-%d.tar.gz", System.currentTimeMillis());
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
    ctx1.execute(
        "create database ? plocal users(admin identified by 'admin' role admin)", "db-to-export");
    final ODatabaseSession session = ctx1.open("db-to-export", "admin", "admin");
    session.createClass("testa");
    final ODatabaseExport export =
        new ODatabaseExport((ODatabaseDocumentInternal) session, exportFileName, iText -> {});
    export.exportDatabase();
    export.close();
    session.close();

    ctx1.execute(
        "create database ? plocal users(admin identified by 'admin' role admin)", "imported-db");
    final ODatabaseSession session1 = ctx1.open("imported-db", "admin", "admin");
    final ODatabaseImport imp =
        new ODatabaseImport((ODatabaseDocumentInternal) session1, exportFileName, iText -> {});
    imp.importDatabase();
    imp.close();
    session1.close();

    final OrientDB ctx2 = setup.getServer(server1).getServerInstance().getContext();
    final ODatabaseSession session2 = ctx2.open("imported-db", "admin", "admin");
    assertTrue(session2.getMetadata().getSchema().existsClass("testa"));
    session2.close();
  }

  @After
  public void after() {
    try {
      setup.getServer(server0).getServerInstance().dropDatabase("db-to-export");
      setup.getServer(server0).getServerInstance().dropDatabase("imported-db");
    } finally {
      setup.teardown();
      File file = new File(exportFileName);
      if (file.exists()) file.delete();
      ODatabaseDocumentTx.closeAll();
    }
  }
}
