package com.orientechnologies.orient.server.tx;

import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.db.ODatabaseType;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.OEdge;
import com.orientechnologies.orient.core.record.OElement;
import com.orientechnologies.orient.core.record.OVertex;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.network.ORemoteImportTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;

import static org.junit.Assert.*;

/**
 * Created by tglman on 03/01/17.
 */
public class RemoteTransactionSupportTest {

  private static final String SERVER_DIRECTORY = "./target/transaction";
  private OServer           server;
  private OrientDB          orientDB;
  private ODatabaseDocument database;

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

    OClass klass = database.createClass("IndexedTx");
    klass.createProperty("name", OType.STRING).createIndex(OClass.INDEX_TYPE.NOTUNIQUE);

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
  public void testResetUpdatedInTxTransaction() {
    database.begin();

    ODocument doc1 = new ODocument();
    doc1.setProperty("name", "Jane");
    database.save(doc1);
    ODocument doc2 = new ODocument("SomeTx");
    doc2.setProperty("name", "Jane");
    database.save(doc2);
    OResultSet result = database.command("update SomeTx set name='July' where name = 'Jane' ");
    assertEquals((long) result.next().getProperty("count"), 1L);
    assertEquals(doc2.getProperty("name"), "July");
    result.close();
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
    result.close();
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
    result.close();
    database.rollback();

    OResultSet result1 = database.command("select count(*) from SomeTx where name='Jane'");
    assertTrue(result1.hasNext());
    assertEquals((long) result1.next().getProperty("count(*)"), 1L);
    result1.close();

  }

  @Test
  public void testRollbackTxCheckStatusTransaction() {
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
    result.close();
    database.rollback();

    OResultSet result1 = database.command("select count(*) from SomeTx where name='Jane'");
    assertTrue(result1.hasNext());
    assertEquals((long) result1.next().getProperty("count(*)"), 1L);

    assertFalse(database.getTransaction().isActive());
    result1.close();

  }

  @Test
  public void testDownloadTransactionAtStart() {
    database.begin();

    database.command("insert into SomeTx set name ='Jane' ").close();
    ;
    assertEquals(database.getTransaction().getEntryCount(), 1);
  }

  @Test
  public void testQueryUpdateCreatedInTxSQLTransaction() {
    database.begin();

    database.command("insert into SomeTx set name ='Jane' ").close();

    OResultSet result = database.command("update SomeTx set name='July' where name = 'Jane' ");
    assertTrue(result.hasNext());
    assertEquals((long) result.next().getProperty("count"), 1L);
    result.close();
    OResultSet result1 = database.query("select from SomeTx where name='July'");
    assertTrue(result1.hasNext());
    assertEquals(result1.next().getProperty("name"), "July");
    assertFalse(result.hasNext());
    result1.close();
  }

  @Test
  public void testQueryDeleteTxSQLTransaction() {

    OElement someTx = database.newElement("SomeTx");
    someTx.setProperty("name", "foo");

    someTx.save();

    database.begin();

    database.command("delete from SomeTx");

    database.commit();

    OResultSet result = database.command("select from SomeTx");
    assertFalse(result.hasNext());
    result.close();

  }

  @Test
  public void testUpdateCreatedInTxIndexGetTransaction() {
    OIndex<?> index = database.getClass("IndexedTx").getProperty("name").getAllIndexes().iterator().next();
    database.begin();
    ODocument doc1 = new ODocument("IndexedTx");
    doc1.setProperty("name", "Jane");
    database.save(doc1);

    OResultSet result = database.command("update IndexedTx set name='July' where name = 'Jane' ");
    assertTrue(result.hasNext());
    assertEquals((long) result.next().getProperty("count"), 1L);
    Collection<OIdentifiable> entry = (Collection<OIdentifiable>) index.get("July");
    assertEquals(entry.size(), 1);
    result.close();
    database.commit();

    entry = (Collection<OIdentifiable>) index.get("July");
    assertEquals(entry.size(), 1);

  }

  @Test
  public void testGenerateIdCounterTransaction() {
    database.begin();

    ODocument doc = new ODocument("SomeTx");
    doc.setProperty("name", "Jane");
    database.save(doc);

    database.command("insert into SomeTx set name ='Jane1' ").close();
    database.command("insert into SomeTx set name ='Jane2' ").close();

    ODocument doc1 = new ODocument("SomeTx");
    doc1.setProperty("name", "Jane3");
    database.save(doc1);

    doc1 = new ODocument("SomeTx");
    doc1.setProperty("name", "Jane4");
    database.save(doc1);
    database.command("insert into SomeTx set name ='Jane2' ").close();

    OResultSet result = database.command("select count(*) from SomeTx");
    System.out.println(result.getExecutionPlan().toString());
    assertTrue(result.hasNext());
    assertEquals((long) result.next().getProperty("count(*)"), 6L);
    result.close();
    assertTrue(database.getTransaction().isActive());

    database.commit();

    OResultSet result1 = database.command("select count(*) from SomeTx ");
    assertTrue(result1.hasNext());
    assertEquals((long) result1.next().getProperty("count(*)"), 6L);
    result1.close();
    assertFalse(database.getTransaction().isActive());

  }

  @Test
  public void testGraphInTx() {
    database.createVertexClass("MyV");
    database.createEdgeClass("MyE");
    database.begin();

    OVertex v1 = database.newVertex("MyV");
    OVertex v2 = database.newVertex("MyV");
    OEdge edge = v1.addEdge(v2, "MyE");
    edge.setProperty("some", "value");
    database.save(v1);
    OResultSet result1 = database.query("select out_MyE from MyV  where out_MyE is not null");
    assertTrue(result1.hasNext());
    ArrayList<Object> val = new ArrayList<>();
    val.add(edge.getIdentity());
    assertEquals(result1.next().getProperty("out_MyE"), val);
    result1.close();
  }

  @After
  public void after() {
    database.close();
    orientDB.close();
    server.shutdown();
    Orient.instance().startup();
  }

}
