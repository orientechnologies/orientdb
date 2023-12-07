package com.orientechnologies.orient.server.tx;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.orientechnologies.orient.core.config.OGlobalConfiguration;
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
import com.orientechnologies.orient.server.BaseServerMemoryDatabase;
import com.orientechnologies.orient.server.OClientConnection;
import java.util.ArrayList;
import org.junit.Test;

/** Created by tglman on 03/01/17. */
public class RemoteTransactionSupportTest extends BaseServerMemoryDatabase {

  private static final String FIELD_VALUE = "VALUE";

  public void beforeTest() {
    OGlobalConfiguration.CLASS_MINIMUM_CLUSTERS.setValue(1);
    super.beforeTest();

    db.createClass("SomeTx");
    db.createClass("SomeTx2");

    OClass klass = db.createClass("IndexedTx");
    klass.createProperty("name", OType.STRING).createIndex(OClass.INDEX_TYPE.NOTUNIQUE);

    OClass uniqueClass = db.createClass("UniqueIndexedTx");
    uniqueClass.createProperty("name", OType.STRING).createIndex(OClass.INDEX_TYPE.UNIQUE);
  }

  @Test
  public void testQueryUpdateUpdatedInTxTransaction() {
    ODocument doc = new ODocument("SomeTx");
    doc.setProperty("name", "Joe");
    OIdentifiable id = db.save(doc);
    db.begin();
    ODocument doc2 = db.load(id.getIdentity());
    doc2.setProperty("name", "Jane");
    db.save(doc2);
    OResultSet result = db.command("update SomeTx set name='July' where name = 'Jane' ");
    assertEquals((long) result.next().getProperty("count"), 1L);
    ODocument doc3 = db.load(id.getIdentity());
    assertEquals(doc3.getProperty("name"), "July");
  }

  @Test
  public void testResetUpdatedInTxTransaction() {
    db.begin();

    ODocument doc1 = new ODocument();
    doc1.setProperty("name", "Jane");
    db.save(doc1);
    ODocument doc2 = new ODocument("SomeTx");
    doc2.setProperty("name", "Jane");
    db.save(doc2);
    OResultSet result = db.command("update SomeTx set name='July' where name = 'Jane' ");
    assertEquals((long) result.next().getProperty("count"), 1L);
    assertEquals(doc2.getProperty("name"), "July");
    result.close();
  }

  @Test
  public void testQueryUpdateCreatedInTxTransaction() throws InterruptedException {
    db.begin();
    ODocument doc1 = new ODocument("SomeTx");
    doc1.setProperty("name", "Jane");
    OIdentifiable id = db.save(doc1);

    ODocument docx = new ODocument("SomeTx2");
    docx.setProperty("name", "Jane");
    db.save(docx);

    OResultSet result = db.command("update SomeTx set name='July' where name = 'Jane' ");
    assertTrue(result.hasNext());
    assertEquals((long) result.next().getProperty("count"), 1L);
    ODocument doc2 = db.load(id.getIdentity());
    assertEquals(doc2.getProperty("name"), "July");
    assertFalse(result.hasNext());
    result.close();
  }

  @Test
  public void testRollbackTxTransaction() {
    ODocument doc = new ODocument("SomeTx");
    doc.setProperty("name", "Jane");
    db.save(doc);

    db.begin();
    ODocument doc1 = new ODocument("SomeTx");
    doc1.setProperty("name", "Jane");
    db.save(doc1);

    OResultSet result = db.command("update SomeTx set name='July' where name = 'Jane' ");
    assertTrue(result.hasNext());
    assertEquals((long) result.next().getProperty("count"), 2L);
    result.close();
    db.rollback();

    OResultSet result1 = db.command("select count(*) from SomeTx where name='Jane'");
    assertTrue(result1.hasNext());
    assertEquals((long) result1.next().getProperty("count(*)"), 1L);
    result1.close();
  }

  @Test
  public void testRollbackTxCheckStatusTransaction() {
    ODocument doc = new ODocument("SomeTx");
    doc.setProperty("name", "Jane");
    db.save(doc);

    db.begin();
    ODocument doc1 = new ODocument("SomeTx");
    doc1.setProperty("name", "Jane");
    db.save(doc1);

    OResultSet result = db.command("select count(*) from SomeTx where name='Jane' ");
    assertTrue(result.hasNext());
    assertEquals((long) result.next().getProperty("count(*)"), 2L);

    assertTrue(db.getTransaction().isActive());
    result.close();
    db.rollback();

    OResultSet result1 = db.command("select count(*) from SomeTx where name='Jane'");
    assertTrue(result1.hasNext());
    assertEquals((long) result1.next().getProperty("count(*)"), 1L);

    assertFalse(db.getTransaction().isActive());
    result1.close();
  }

  @Test
  public void testDownloadTransactionAtStart() {
    db.begin();

    db.command("insert into SomeTx set name ='Jane' ").close();
    ;
    assertEquals(db.getTransaction().getEntryCount(), 1);
  }

  @Test
  public void testQueryUpdateCreatedInTxSQLTransaction() {
    db.begin();

    db.command("insert into SomeTx set name ='Jane' ").close();

    OResultSet result = db.command("update SomeTx set name='July' where name = 'Jane' ");
    assertTrue(result.hasNext());
    assertEquals((long) result.next().getProperty("count"), 1L);
    result.close();
    OResultSet result1 = db.query("select from SomeTx where name='July'");
    assertTrue(result1.hasNext());
    assertEquals(result1.next().getProperty("name"), "July");
    assertFalse(result.hasNext());
    result1.close();
  }

  @Test
  public void testQueryDeleteTxSQLTransaction() {

    OElement someTx = db.newElement("SomeTx");
    someTx.setProperty("name", "foo");

    someTx.save();

    db.begin();

    db.command("delete from SomeTx");

    db.commit();

    OResultSet result = db.command("select from SomeTx");
    assertFalse(result.hasNext());
    result.close();
  }

  @Test
  public void testDoubleSaveTransaction() {
    db.begin();
    OElement someTx = db.newElement("SomeTx");
    someTx.setProperty("name", "foo");
    db.save(someTx);
    db.save(someTx);
    assertEquals(db.getTransaction().getEntryCount(), 1);
    assertEquals(db.countClass("SomeTx"), 1);
    db.commit();
    assertEquals(db.countClass("SomeTx"), 1);
  }

  @Test
  public void testDoubleSaveDoubleFlushTransaction() {
    db.begin();
    OElement someTx = db.newElement("SomeTx");
    someTx.setProperty("name", "foo");
    db.save(someTx);
    db.save(someTx);
    OResultSet result = db.query("select from SomeTx");
    assertEquals(1, result.stream().count());
    result.close();
    db.save(someTx);
    db.save(someTx);
    result = db.query("select from SomeTx");
    assertEquals(1, result.stream().count());
    result.close();
    assertEquals(db.getTransaction().getEntryCount(), 1);
    assertEquals(db.countClass("SomeTx"), 1);
    db.commit();
    assertEquals(db.countClass("SomeTx"), 1);
  }

  @Test
  public void testRefFlushedInTransaction() {
    db.begin();
    OElement someTx = db.newElement("SomeTx");
    someTx.setProperty("name", "foo");
    db.save(someTx);

    OElement oneMore = db.newElement("SomeTx");
    oneMore.setProperty("name", "bar");
    oneMore.setProperty("ref", someTx);
    OResultSet result = db.query("select from SomeTx");
    assertEquals(1, result.stream().count());
    result.close();
    db.save(oneMore);
    db.commit();
    OResultSet result1 = db.query("select ref from SomeTx where name='bar'");
    assertTrue(result1.hasNext());
    assertEquals(someTx.getIdentity(), result1.next().getProperty("ref"));
    result1.close();
  }

  @Test
  public void testDoubleRefFlushedInTransaction() {
    db.begin();
    OElement someTx = db.newElement("SomeTx");
    someTx.setProperty("name", "foo");
    db.save(someTx);

    OElement oneMore = db.newElement("SomeTx");
    oneMore.setProperty("name", "bar");
    oneMore.setProperty("ref", someTx.getIdentity());

    OResultSet result = db.query("select from SomeTx");
    assertEquals(1, result.stream().count());
    result.close();

    OElement ref2 = db.newElement("SomeTx");
    ref2.setProperty("name", "other");
    db.save(ref2);

    oneMore.setProperty("ref2", ref2.getIdentity());
    result = db.query("select from SomeTx");
    assertEquals(2, result.stream().count());
    result.close();

    db.save(oneMore);
    OResultSet result1 = db.query("select ref,ref2 from SomeTx where name='bar'");
    assertTrue(result1.hasNext());
    OResult next = result1.next();
    assertEquals(someTx.getIdentity(), next.getProperty("ref"));
    assertEquals(ref2.getIdentity(), next.getProperty("ref2"));
    result1.close();

    db.commit();
    result1 = db.query("select ref,ref2 from SomeTx where name='bar'");
    assertTrue(result1.hasNext());
    next = result1.next();
    assertEquals(someTx.getIdentity(), next.getProperty("ref"));
    assertEquals(ref2.getIdentity(), next.getProperty("ref2"));
    result1.close();
  }

  @Test
  public void testGenerateIdCounterTransaction() {
    db.begin();

    ODocument doc = new ODocument("SomeTx");
    doc.setProperty("name", "Jane");
    db.save(doc);

    db.command("insert into SomeTx set name ='Jane1' ").close();
    db.command("insert into SomeTx set name ='Jane2' ").close();

    ODocument doc1 = new ODocument("SomeTx");
    doc1.setProperty("name", "Jane3");
    db.save(doc1);

    doc1 = new ODocument("SomeTx");
    doc1.setProperty("name", "Jane4");
    db.save(doc1);
    db.command("insert into SomeTx set name ='Jane2' ").close();

    OResultSet result = db.command("select count(*) from SomeTx");
    System.out.println(result.getExecutionPlan().toString());
    assertTrue(result.hasNext());
    assertEquals((long) result.next().getProperty("count(*)"), 6L);
    result.close();
    assertTrue(db.getTransaction().isActive());

    db.commit();

    OResultSet result1 = db.command("select count(*) from SomeTx ");
    assertTrue(result1.hasNext());
    assertEquals((long) result1.next().getProperty("count(*)"), 6L);
    result1.close();
    assertFalse(db.getTransaction().isActive());
  }

  @Test
  public void testGraphInTx() {
    db.createVertexClass("MyV");
    db.createEdgeClass("MyE");
    db.begin();

    OVertex v1 = db.newVertex("MyV");
    OVertex v2 = db.newVertex("MyV");
    OEdge edge = v1.addEdge(v2, "MyE");
    edge.setProperty("some", "value");
    db.save(v1);
    OResultSet result1 = db.query("select out_MyE from MyV  where out_MyE is not null");
    assertTrue(result1.hasNext());
    ArrayList<Object> val = new ArrayList<>();
    val.add(edge.getIdentity());
    assertEquals(result1.next().getProperty("out_MyE"), val);
    result1.close();
  }

  @Test
  public void testRidbagsTx() {
    db.begin();

    OElement v1 = db.newElement("SomeTx");
    OElement v2 = db.newElement("SomeTx");
    db.save(v2);
    ORidBag ridbag = new ORidBag();
    ridbag.add(v2.getIdentity());
    v1.setProperty("rids", ridbag);
    db.save(v1);
    OResultSet result1 = db.query("select rids from SomeTx where rids is not null");
    assertTrue(result1.hasNext());
    OElement v3 = db.newElement("SomeTx");
    db.save(v3);
    ArrayList<Object> val = new ArrayList<>();
    val.add(v2.getIdentity());
    assertEquals(result1.next().getProperty("rids"), val);
    result1.close();
    result1 = db.query("select rids from SomeTx where rids is not null");
    assertTrue(result1.hasNext());
    assertEquals(result1.next().getProperty("rids"), val);
    result1.close();
  }

  @Test
  public void testProperIndexingOnDoubleInternalBegin() {
    db.begin(OTransaction.TXTYPE.OPTIMISTIC);

    OElement idx = db.newElement("IndexedTx");
    idx.setProperty("name", FIELD_VALUE);
    db.save(idx);
    OElement someTx = db.newElement("SomeTx");
    someTx.setProperty("name", "foo");
    ORecord id = db.save(someTx);
    try (OResultSet rs = db.query("select from ?", id)) {}

    db.commit();

    // nothing is found (unexpected behaviour)
    try (OResultSet rs = db.query("select * from IndexedTx where name = ?", FIELD_VALUE)) {
      assertEquals(rs.stream().count(), 1);
    }
  }

  @Test(expected = ORecordDuplicatedException.class)
  public void testDuplicateIndexTx() {
    db.begin();

    OElement v1 = db.newElement("UniqueIndexedTx");
    v1.setProperty("name", "a");
    db.save(v1);

    OElement v2 = db.newElement("UniqueIndexedTx");
    v2.setProperty("name", "a");
    db.save(v2);
    db.commit();
  }

  @Test
  public void testKilledSession() {
    db.begin();
    OElement v2 = db.newElement("SomeTx");
    v2.setProperty("name", "a");
    db.save(v2);

    OResultSet result1 = db.query("select rids from SomeTx ");
    assertTrue(result1.hasNext());
    result1.close();

    for (OClientConnection conn : server.getClientConnectionManager().getConnections()) {
      conn.close();
    }
    db.activateOnCurrentThread();

    db.commit();
    result1 = db.query("select rids from SomeTx ");
    assertTrue(result1.hasNext());
    result1.close();
  }
}
