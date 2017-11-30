package com.orientechnologies.distribution.integration;

import com.orientechnologies.orient.console.OConsoleDatabaseApp;
import com.orientechnologies.orient.core.db.ODatabasePool;
import com.orientechnologies.orient.core.db.ODatabaseType;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.OServerMain;
import org.testng.annotations.*;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * This abstract class is a template to be extended to implements integration tests.
 * <p>
 * <p>
 * Created by frank on 15/03/2017.
 */
public abstract class OIntegrationTestTemplate {
  private static final String ORIENTDB_HOME          = "ORIENTDB_HOME";
  private static final String ORIENTDB_ROOT_PASSWORD = "ORIENTDB_ROOT_PASSWORD";

  private static volatile OServer server;
  private static volatile String  beforeOrientDBHome;
  private static volatile String  beforeRootPassword;

  protected               ODatabaseDocument db;
  static volatile         OrientDB          orientDB;
  private static volatile ODatabasePool     pool;

  @BeforeSuite
  public static void beforeSuite() throws Exception {
    final String buildDirectory = System.getProperty("buildDirectory", "target");
    final Path buildPath = Paths.get(buildDirectory);

    final String buildName = System.getProperty("buildName", "unknownBuild");

    final Path orientdbHomePath = buildPath.resolve(buildName + ".dir").resolve(buildName);
    Files.createDirectories(orientdbHomePath.resolve("databases"));

    beforeOrientDBHome = System.getProperty(ORIENTDB_HOME);
    beforeRootPassword = System.getProperty(ORIENTDB_ROOT_PASSWORD);

    System.setProperty(ORIENTDB_ROOT_PASSWORD, "root");
    System.setProperty(ORIENTDB_HOME, orientdbHomePath.toRealPath().toString());

    server = OServerMain.create(false);
    try (InputStream configuration = OIntegrationTestTemplate.class.getResourceAsStream("/orientdb-server-config.xml")) {
      server.startup(configuration);
    }
    server.activate();

    createDemoDB();
  }

  private static void createDemoDB() throws IOException {
    orientDB = new OrientDB("remote:localhost:9595", "root", "root", OrientDBConfig.defaultConfig());
    if (!orientDB.exists("demodb")) {
      orientDB.create("demodb", ODatabaseType.PLOCAL);

      final String loadScript = System.getProperty("loadScriptPath");
      if (loadScript != null) {
        final Path loadScriptPath = Paths.get(loadScript);
        final OConsoleDatabaseApp console = new OConsoleDatabaseApp(new String[] { loadScriptPath.toRealPath().toString() });
        console.connect("remote:localhost:9595/demodb", "admin", "admin");
        console.run();
        console.close();
      }

    }

    pool = new ODatabasePool(orientDB, "demodb", "admin", "admin");
  }

  @AfterSuite
  public static void afterSuite() {
    orientDB.close();
    pool.close();

    server.shutdown();

    if (beforeRootPassword == null) {
      System.clearProperty(ORIENTDB_ROOT_PASSWORD);
    } else {
      System.setProperty(ORIENTDB_ROOT_PASSWORD, beforeRootPassword);
    }

    if (beforeOrientDBHome == null) {
      System.clearProperty(ORIENTDB_HOME);
    } else {
      System.setProperty(ORIENTDB_HOME, beforeOrientDBHome);
    }
  }

  @BeforeMethod
  public void before() {
    db = pool.acquire();
  }

  @AfterMethod
  public void after() {
    db.activateOnCurrentThread();
    db.close();
  }
}
