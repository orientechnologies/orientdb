package com.orientechnologies.orient.server.metadata;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import com.orientechnologies.common.io.OFileUtils;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.db.ODatabase;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.server.OServer;
import java.io.File;
import java.util.Locale;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/** Created by tglman on 23/05/17. */
public class MetadataPushTest {

  private static final String SERVER_DIRECTORY = "./target/metadata-push";
  private OServer server;
  private OrientDB orientDB;
  private ODatabaseDocument database;

  private OrientDB secondOrientDB;
  private ODatabaseDocumentInternal secondDatabase;

  @Before
  public void before() throws Exception {
    server = new OServer(false);
    server.setServerRootDirectory(SERVER_DIRECTORY);
    server.startup(getClass().getResourceAsStream("orientdb-server-config.xml"));
    server.activate();

    orientDB = new OrientDB("remote:localhost", "root", "root", OrientDBConfig.defaultConfig());
    orientDB.execute(
        "create database ? memory users (admin identified by 'admin' role admin)",
        MetadataPushTest.class.getSimpleName());
    database = orientDB.open(MetadataPushTest.class.getSimpleName(), "admin", "admin");

    secondOrientDB = new OrientDB("remote:localhost", OrientDBConfig.defaultConfig());
    secondDatabase =
        (ODatabaseDocumentInternal)
            orientDB.open(MetadataPushTest.class.getSimpleName(), "admin", "admin");
  }

  @After
  public void after() {
    database.activateOnCurrentThread();
    database.close();
    orientDB.close();
    secondDatabase.activateOnCurrentThread();
    secondDatabase.close();
    secondOrientDB.close();
    server.shutdown();

    Orient.instance().shutdown();
    OFileUtils.deleteRecursively(new File(SERVER_DIRECTORY));
    Orient.instance().startup();
  }

  @Test
  public void testStorageUpdate() throws InterruptedException {
    database.activateOnCurrentThread();
    database.command(" ALTER DATABASE LOCALELANGUAGE  ?", Locale.GERMANY.getLanguage());
    // Push done in background for now, do not guarantee update before command return.
    Thread.sleep(500);
    secondDatabase.activateOnCurrentThread();
    assertEquals(
        secondDatabase.get(ODatabase.ATTRIBUTES.LOCALELANGUAGE), Locale.GERMANY.getLanguage());
  }

  @Test
  public void testSchemaUpdate() throws InterruptedException {
    database.activateOnCurrentThread();
    database.command(" create class X");
    // Push done in background for now, do not guarantee update before command return.
    Thread.sleep(500);
    secondDatabase.activateOnCurrentThread();
    assertTrue(secondDatabase.getMetadata().getSchema().existsClass("X"));
  }

  @Test
  public void testIndexManagerUpdate() throws InterruptedException {
    database.activateOnCurrentThread();
    database.command(" create class X");
    database.command(" create property X.y STRING");
    database.command(" create index X.y on X(y) NOTUNIQUE");
    // Push done in background for now, do not guarantee update before command return.
    Thread.sleep(500);
    secondDatabase.activateOnCurrentThread();
    assertTrue(secondDatabase.getMetadata().getIndexManagerInternal().existsIndex("X.y"));
  }

  @Test
  public void testFunctionUpdate() throws InterruptedException {
    database.activateOnCurrentThread();
    database.command("CREATE FUNCTION test \"print('\\nTest!')\"");
    // Push done in background for now, do not guarantee update before command return.
    Thread.sleep(500);
    secondDatabase.activateOnCurrentThread();
    assertNotNull(secondDatabase.getMetadata().getFunctionLibrary().getFunction("test"));
  }

  @Test
  public void testSequencesUpdate() throws InterruptedException {
    database.activateOnCurrentThread();
    database.command("CREATE SEQUENCE test TYPE CACHED");
    // Push done in background for now, do not guarantee update before command return.
    Thread.sleep(500);
    secondDatabase.activateOnCurrentThread();
    assertNotNull(secondDatabase.getMetadata().getSequenceLibrary().getSequence("test"));
  }
}
