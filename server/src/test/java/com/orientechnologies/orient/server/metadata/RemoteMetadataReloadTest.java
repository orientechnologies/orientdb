package com.orientechnologies.orient.server.metadata;

import com.orientechnologies.common.io.OFileUtils;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.db.ODatabase;
import com.orientechnologies.orient.core.db.ODatabaseType;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.server.OServer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.Locale;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class RemoteMetadataReloadTest {
  private static final String SERVER_DIRECTORY = "./target/metadata-reload";
  private OServer           server;
  private OrientDB          orientDB;
  private ODatabaseDocument database;

  @Before
  public void before() throws Exception {
    server = new OServer(false);
    server.setServerRootDirectory(SERVER_DIRECTORY);
    server.startup(getClass().getResourceAsStream("orientdb-server-config.xml"));
    server.activate();

    orientDB = new OrientDB("remote:localhost", "root", "root", OrientDBConfig.defaultConfig());
    orientDB.create(MetadataPushTest.class.getSimpleName(), ODatabaseType.MEMORY);
    database = orientDB.open(MetadataPushTest.class.getSimpleName(), "admin", "admin");

  }

  @After
  public void after() {
    database.close();
    orientDB.close();
    server.shutdown();

    Orient.instance().shutdown();
    OFileUtils.deleteRecursively(new File(SERVER_DIRECTORY));
    Orient.instance().startup();
  }

  @Test
  public void testStorageUpdate() throws InterruptedException {
    database.command(" ALTER DATABASE LOCALELANGUAGE  ?", Locale.GERMANY.getLanguage());
    assertEquals(database.get(ODatabase.ATTRIBUTES.LOCALELANGUAGE), Locale.GERMANY.getLanguage());
  }

  @Test
  public void testSchemaUpdate() throws InterruptedException {
    database.command(" create class X");
    assertTrue(database.getMetadata().getSchema().existsClass("X"));
  }

  @Test
  public void testIndexManagerUpdate() throws InterruptedException {
    database.command(" create class X");
    database.command(" create property X.y STRING");
    database.command(" create index X.y on X(y) NOTUNIQUE");
    assertTrue(database.getMetadata().getIndexManager().existsIndex("X.y"));
  }

  @Test
  public void testFunctionUpdate() throws InterruptedException {
    database.command("CREATE FUNCTION test \"print('\\nTest!')\"");
    assertNotNull(database.getMetadata().getFunctionLibrary().getFunction("test"));
  }

  @Test
  public void testSequencesUpdate() throws InterruptedException {
    database.command("CREATE SEQUENCE test TYPE CACHED");
    assertNotNull(database.getMetadata().getSequenceLibrary().getSequence("test"));
  }
}
