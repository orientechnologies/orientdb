package com.orientechnologies.orient.server.tx;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.orientechnologies.common.io.OFileUtils;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.db.record.ridbag.ORidBag;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.OEdge;
import com.orientechnologies.orient.core.record.OElement;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.OVertex;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import com.orientechnologies.orient.core.storage.ORecordDuplicatedException;
import com.orientechnologies.orient.core.tx.OTransaction;
import com.orientechnologies.orient.server.OClientConnection;
import com.orientechnologies.orient.server.OServer;
import java.io.File;
import java.util.ArrayList;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/** Created by tglman on 03/01/17. */
public class RemoteTransactionSupportTest {

  private static final String CLASS_1 = "SomeClass";
  private static final String CLASS_2 = "AnotherClass";
  private static final String EDGE = "SomeEdge";
  private static final String FIELD_VALUE = "VALUE";
  private static final String SERVER_DIRECTORY = "./target/transaction";
  private OServer server;
  private OrientDB orientDB;
  private ODatabaseDocument database;

  @Before
  public void before() throws Exception {
    OGlobalConfiguration.CLASS_MINIMUM_CLUSTERS.setValue(1);
    server = new OServer(false);
    server.setServerRootDirectory(SERVER_DIRECTORY);
    server.startup(getClass().getResourceAsStream("orientdb-server-config.xml"));
    server.activate();

    orientDB = new OrientDB("remote:localhost", "root", "root", OrientDBConfig.defaultConfig());
    orientDB.execute(
        "create database ? memory users (admin identified by 'admin' role admin)",
        RemoteTransactionSupportTest.class.getSimpleName());
    database = orientDB.open(RemoteTransactionSupportTest.class.getSimpleName(), "admin", "admin");
    database.createClass("SomeTx");
    database.createClass("SomeTx2");

    OClass klass = database.createClass("IndexedTx");
    klass.createProperty("name", OType.STRING).createIndex(OClass.INDEX_TYPE.NOTUNIQUE);

    OClass uniqueClass = database.createClass("UniqueIndexedTx");
    uniqueClass.createProperty("name", OType.STRING).createIndex(OClass.INDEX_TYPE.UNIQUE);
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
  public void testQueryUpdateCreatedInTxTransaction() throws InterruptedException {
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
  public void testDoubleSaveTransaction() {
    database.begin();
    OElement someTx = database.newElement("SomeTx");
    someTx.setProperty("name", "foo");
    database.save(someTx);
    database.save(someTx);
    assertEquals(database.getTransaction().getEntryCount(), 1);
    assertEquals(database.countClass("SomeTx"), 1);
    database.commit();
    assertEquals(database.countClass("SomeTx"), 1);
  }

  @Test
  public void testDoubleSaveDoubleFlushTransaction() {
    database.begin();
    OElement someTx = database.newElement("SomeTx");
    someTx.setProperty("name", "foo");
    database.save(someTx);
    database.save(someTx);
    OResultSet result = database.query("select from SomeTx");
    assertEquals(1, result.stream().count());
    result.close();
    database.save(someTx);
    database.save(someTx);
    result = database.query("select from SomeTx");
    assertEquals(1, result.stream().count());
    result.close();
    assertEquals(database.getTransaction().getEntryCount(), 1);
    assertEquals(database.countClass("SomeTx"), 1);
    database.commit();
    assertEquals(database.countClass("SomeTx"), 1);
  }

  @Test
  public void testRefFlushedInTransaction() {
    database.begin();
    OElement someTx = database.newElement("SomeTx");
    someTx.setProperty("name", "foo");
    database.save(someTx);

    OElement oneMore = database.newElement("SomeTx");
    oneMore.setProperty("name", "bar");
    oneMore.setProperty("ref", someTx);
    OResultSet result = database.query("select from SomeTx");
    assertEquals(1, result.stream().count());
    result.close();
    database.save(oneMore);
    database.commit();
    OResultSet result1 = database.query("select ref from SomeTx where name='bar'");
    assertTrue(result1.hasNext());
    assertEquals(someTx.getIdentity(), result1.next().getProperty("ref"));
    result1.close();
  }

  @Test
  public void testDoubleRefFlushedInTransaction() {
    database.begin();
    OElement someTx = database.newElement("SomeTx");
    someTx.setProperty("name", "foo");
    database.save(someTx);

    OElement oneMore = database.newElement("SomeTx");
    oneMore.setProperty("name", "bar");
    oneMore.setProperty("ref", someTx.getIdentity());

    OResultSet result = database.query("select from SomeTx");
    assertEquals(1, result.stream().count());
    result.close();

    OElement ref2 = database.newElement("SomeTx");
    ref2.setProperty("name", "other");
    database.save(ref2);

    oneMore.setProperty("ref2", ref2.getIdentity());
    result = database.query("select from SomeTx");
    assertEquals(2, result.stream().count());
    result.close();

    database.save(oneMore);
    OResultSet result1 = database.query("select ref,ref2 from SomeTx where name='bar'");
    assertTrue(result1.hasNext());
    OResult next = result1.next();
    assertEquals(someTx.getIdentity(), next.getProperty("ref"));
    assertEquals(ref2.getIdentity(), next.getProperty("ref2"));
    result1.close();

    database.commit();
    result1 = database.query("select ref,ref2 from SomeTx where name='bar'");
    assertTrue(result1.hasNext());
    next = result1.next();
    assertEquals(someTx.getIdentity(), next.getProperty("ref"));
    assertEquals(ref2.getIdentity(), next.getProperty("ref2"));
    result1.close();
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

  @Test
  public void testRidbagsTx() {
    database.begin();

    OElement v1 = database.newElement("SomeTx");
    OElement v2 = database.newElement("SomeTx");
    database.save(v2);
    ORidBag ridbag = new ORidBag();
    ridbag.add(v2.getIdentity());
    v1.setProperty("rids", ridbag);
    database.save(v1);
    OResultSet result1 = database.query("select rids from SomeTx where rids is not null");
    assertTrue(result1.hasNext());
    OElement v3 = database.newElement("SomeTx");
    database.save(v3);
    ArrayList<Object> val = new ArrayList<>();
    val.add(v2.getIdentity());
    assertEquals(result1.next().getProperty("rids"), val);
    result1.close();
    result1 = database.query("select rids from SomeTx where rids is not null");
    assertTrue(result1.hasNext());
    assertEquals(result1.next().getProperty("rids"), val);
    result1.close();
  }

  @Test
  public void testProperIndexingOnDoubleInternalBegin() {
    database.begin(OTransaction.TXTYPE.OPTIMISTIC);

    OElement idx = database.newElement("IndexedTx");
    idx.setProperty("name", FIELD_VALUE);
    database.save(idx);
    OElement someTx = database.newElement("SomeTx");
    someTx.setProperty("name", "foo");
    ORecord id = database.save(someTx);
    try (OResultSet rs = database.query("select from ?", id)) {}

    database.commit();

    // nothing is found (unexpected behaviour)
    try (OResultSet rs = database.query("select * from IndexedTx where name = ?", FIELD_VALUE)) {
      assertEquals(rs.stream().count(), 1);
    }
  }

  @Test(expected = ORecordDuplicatedException.class)
  public void testDuplicateIndexTx() {
    database.begin();

    OElement v1 = database.newElement("UniqueIndexedTx");
    v1.setProperty("name", "a");
    database.save(v1);

    OElement v2 = database.newElement("UniqueIndexedTx");
    v2.setProperty("name", "a");
    database.save(v2);
    database.commit();
  }

  @Test
  public void testKilledSession() {
    database.begin();
    OElement v2 = database.newElement("SomeTx");
    v2.setProperty("name", "a");
    database.save(v2);

    OResultSet result1 = database.query("select rids from SomeTx ");
    assertTrue(result1.hasNext());
    result1.close();

    for (OClientConnection conn : server.getClientConnectionManager().getConnections()) {
      conn.close();
    }
    database.activateOnCurrentThread();

    database.commit();
    result1 = database.query("select rids from SomeTx ");
    assertTrue(result1.hasNext());
    result1.close();
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
