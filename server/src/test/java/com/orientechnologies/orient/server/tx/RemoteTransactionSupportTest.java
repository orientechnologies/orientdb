package com.orientechnologies.orient.server.tx;

import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.db.OrientDBFactory;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.network.ORemoteImportTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Created by tglman on 03/01/17.
 */
public class RemoteTransactionSupportTest {

  private static final String SERVER_DIRECTORY = "./target/transaction";
  private OServer           server;
  private OrientDBFactory   factory;
  private ODatabaseDocument database;

  @Before
  public void before() throws Exception {
    server = new OServer();
    server.setServerRootDirectory(SERVER_DIRECTORY);
    server.startup(getClass().getResourceAsStream("orientdb-server-config.xml"));
    server.activate();

    factory = OrientDBFactory.remote(new String[] { "localhost" }, OrientDBConfig.defaultConfig());
    factory.create(ORemoteImportTest.class.getSimpleName(), "root", "root", OrientDBFactory.DatabaseType.MEMORY);
    database = factory.open(ORemoteImportTest.class.getSimpleName(), "admin", "admin");
    database.createClass("SomeTx");
    database.createClass("SomeTx2");
  }

  @Test
  public void testQueryUpdateUpdatedInTxTransaction() {
    ODocument doc = new ODocument("SomeTx");
    doc.setProperty("name", "Joe");
    OIdentifiable id = database.save(doc);
    database.begin();
    ODocument doc2 = database.load(id.getIdentity());
    doc2.setProperty("name", "Jane");
    database.save(doc2);
    OResultSet result = database.command("update SomeTx set name='July' where name = 'Jane' ");
    assertEquals((long) result.next().getProperty("count"), 1L);
    ODocument doc3 = database.load(id.getIdentity());
    assertEquals(doc3.getProperty("name"), "July");
  }

  @Test
  public void testQueryUpdateCreatedInTxTransaction() {
    database.begin();
    ODocument doc1 = new ODocument("SomeTx");
    doc1.setProperty("name", "Jane");
    OIdentifiable id = database.save(doc1);

    ODocument docx = new ODocument("SomeTx2");
    docx.setProperty("name", "Jane");
    database.save(doc1);

    OResultSet result = database.command("update SomeTx set name='July' where name = 'Jane' ");
    assertTrue(result.hasNext());
    assertEquals((long) result.next().getProperty("count"), 1L);
    ODocument doc2 = database.load(id.getIdentity());
    assertEquals(doc2.getProperty("name"), "July");
    assertFalse(result.hasNext());
  }

  @After
  public void after() {
    database.close();
    factory.close();
    server.shutdown();
    Orient.instance().startup();
  }

}
