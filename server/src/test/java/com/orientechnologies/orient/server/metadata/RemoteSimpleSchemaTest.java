package com.orientechnologies.orient.server.metadata;

import com.orientechnologies.common.io.OFileUtils;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.db.ODatabaseType;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.server.OServer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class RemoteSimpleSchemaTest {

  private static final String SERVER_DIRECTORY = "./target/metadata-push";
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
    orientDB.create(RemoteSimpleSchemaTest.class.getSimpleName(), ODatabaseType.MEMORY);
    database = orientDB.open(RemoteSimpleSchemaTest.class.getSimpleName(), "admin", "admin");

  }

  @Test
  public void testCreateClassIfNotExist() {
    database.createClassIfNotExist("test");
    assertTrue(database.getMetadata().getSchema().existsClass("test"));
    assertTrue(database.getMetadata().getSchema().existsClass("TEST"));
    database.createClassIfNotExist("TEST");
    database.createClassIfNotExist("test");
  }

  @Test
  public void testNotCaseSensitiveDrop() {
    database.createClass("test");
    assertTrue(database.getMetadata().getSchema().existsClass("test"));
    database.getMetadata().getSchema().dropClass("TEST");
    assertFalse(database.getMetadata().getSchema().existsClass("test"));
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
}
