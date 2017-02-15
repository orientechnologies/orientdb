package com.orientechnologies.orient.server.tx;

import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.db.ODatabaseType;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
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
  private OServer             server;
  private OrientDB            orientDB;
  private ODatabaseDocument   database;

  @Before
  public void before() throws Exception {
    server = new OServer();
    server.setServerRootDirectory(SERVER_DIRECTORY);
    server.startup(getClass().getResourceAsStream("orientdb-server-config.xml"));
    server.activate();

    orientDB = new OrientDB("remote:localhost", "root", "root", OrientDBConfig.defaultConfig());
    orientDB.create(ORemoteImportTest.class.getSimpleName(), ODatabaseType.MEMORY);
    database = orientDB.open(ORemoteImportTest.class.getSimpleName(), "admin", "admin");
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
    database.save(docx);

    OResultSet result = database.command("update SomeTx set name='July' where name = 'Jane' ");
    assertTrue(result.hasNext());
    assertEquals((long) result.next().getProperty("count"), 1L);
    ODocument doc2 = database.load(id.getIdentity());
    assertEquals(doc2.getProperty("name"), "July");
    assertFalse(result.hasNext());
  }

  @Test
  public void testRollbackTxTransaction() {
    ODocument doc = new ODocument("SomeTx");
    doc.setProperty("name", "Jane");
    database.save(doc);

    database.begin();
    ODocument doc1 = new ODocument("SomeTx");
    doc1.setProperty("name", "Jane");
    database.save(doc1);

    OResultSet result = database.command("update SomeTx set name='July' where name = 'Jane' ");
    assertTrue(result.hasNext());
    assertEquals((long) result.next().getProperty("count"), 2L);

    database.rollback();

    OResultSet result1 = database.command("select count(*) from SomeTx where name='Jane'");
    assertTrue(result1.hasNext());
    assertEquals((long) result1.next().getProperty("count(*)"), 1L);

  }

  @Test
  public void testRollbackTxChekcStatusTransaction() {
    ODocument doc = new ODocument("SomeTx");
    doc.setProperty("name", "Jane");
    database.save(doc);

    database.begin();
    ODocument doc1 = new ODocument("SomeTx");
    doc1.setProperty("name", "Jane");
    database.save(doc1);

    OResultSet result = database.command("select count(*) from SomeTx where name='Jane' ");
    assertTrue(result.hasNext());
    assertEquals((long) result.next().getProperty("count(*)"), 2L);

    assertTrue(database.getTransaction().isActive());

    database.rollback();

    OResultSet result1 = database.command("select count(*) from SomeTx where name='Jane'");
    assertTrue(result1.hasNext());
    assertEquals((long) result1.next().getProperty("count(*)"), 1L);

    assertFalse(database.getTransaction().isActive());

  }

  @Test
  public void testQueryUpdateCreatedInTxSQLTransaction() {
    database.begin();

    database.command("insert into SomeTx set name ='Jane' ");

    OResultSet result = database.command("update SomeTx set name='July' where name = 'Jane' ");
    assertTrue(result.hasNext());
    assertEquals((long) result.next().getProperty("count"), 1L);
    OResultSet result1 = database.query("select from SomeTx where name='July'");
    assertTrue(result1.hasNext());
    assertEquals(result1.next().getProperty("name"), "July");
    assertFalse(result.hasNext());
  }

  @After
  public void after() {
    database.close();
    orientDB.close();
    server.shutdown();
    Orient.instance().startup();
  }

}
