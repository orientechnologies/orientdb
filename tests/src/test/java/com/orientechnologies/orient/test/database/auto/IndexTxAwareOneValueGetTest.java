package com.orientechnologies.orient.test.database.auto;

import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.index.OIndexTxAwareOneValue;
import com.orientechnologies.orient.core.sql.OCommandSQL;

@Test
public class IndexTxAwareOneValueGetTest {
  private final ODatabaseDocumentTx database;

  @Parameters(value = "url")
  public IndexTxAwareOneValueGetTest(final String iURL) {
    this.database = new ODatabaseDocumentTx(iURL);
  }

  @BeforeClass
  public void beforeClass() {
    database.open("admin", "admin");

    database.command(new OCommandSQL("create index idxTxAwareOneValueGetTest unique")).execute();
    database.close();
  }

  @BeforeMethod
  public void beforeMethod() {
    database.open("admin", "admin");
  }

  @AfterMethod
  public void afterMethod() {
    database.command(new OCommandSQL("delete from index:idxTxAwareOneValueGetTest")).execute();
    database.close();
  }

  @Test
  public void testPut() {
    database.begin();
    final OIndex<?> index = database.getMetadata().getIndexManager().getIndex("idxTxAwareOneValueGetTest");
    Assert.assertTrue(index instanceof OIndexTxAwareOneValue);

    final int clusterId = database.getDefaultClusterId();
    index.put(1, new ORecordId(clusterId, 1));
    index.put(2, new ORecordId(clusterId, 2));
    database.commit();

    Assert.assertNull(database.getTransaction().getIndexChanges("idxTxAwareOneValueGetTest"));
    Assert.assertNotNull(index.get(1));
    Assert.assertNotNull(index.get(2));

    database.begin();

    index.put(3, new ORecordId(clusterId, 3));

    Assert.assertNotNull(database.getTransaction().getIndexChanges("idxTxAwareOneValueGetTest"));
    Assert.assertNotNull(index.get(3));

    database.rollback();

    Assert.assertNull(database.getTransaction().getIndexChanges("idxTxAwareOneValueGetTest"));
    Assert.assertNotNull(index.get(1));
    Assert.assertNotNull(index.get(2));
    Assert.assertNull(index.get(3));
  }

  @Test
  public void testClear() {
    database.begin();
    final OIndex<?> index = database.getMetadata().getIndexManager().getIndex("idxTxAwareOneValueGetTest");
    Assert.assertTrue(index instanceof OIndexTxAwareOneValue);

    final int clusterId = database.getDefaultClusterId();
    index.put(1, new ORecordId(clusterId, 1));
    index.put(2, new ORecordId(clusterId, 2));
    database.commit();

    Assert.assertNull(database.getTransaction().getIndexChanges("idxTxAwareOneValueGetTest"));
    Assert.assertNotNull(index.get(1));
    Assert.assertNotNull(index.get(2));

    database.begin();

    index.clear();

    Assert.assertNotNull(database.getTransaction().getIndexChanges("idxTxAwareOneValueGetTest"));
    Assert.assertNull(index.get(1));
    Assert.assertNull(index.get(2));

    database.rollback();

    Assert.assertNull(database.getTransaction().getIndexChanges("idxTxAwareOneValueGetTest"));
    Assert.assertNotNull(index.get(1));
    Assert.assertNotNull(index.get(2));
  }

  @Test
  public void testClearAndPut() {
    database.begin();
    final OIndex<?> index = database.getMetadata().getIndexManager().getIndex("idxTxAwareOneValueGetTest");
    Assert.assertTrue(index instanceof OIndexTxAwareOneValue);

    final int clusterId = database.getDefaultClusterId();
    index.put(1, new ORecordId(clusterId, 1));
    index.put(2, new ORecordId(clusterId, 2));
    database.commit();

    Assert.assertNull(database.getTransaction().getIndexChanges("idxTxAwareOneValueGetTest"));
    Assert.assertNotNull(index.get(1));
    Assert.assertNotNull(index.get(2));

    database.begin();

    index.clear();
    index.put(2, new ORecordId(clusterId, 2));

    Assert.assertNotNull(database.getTransaction().getIndexChanges("idxTxAwareOneValueGetTest"));
    Assert.assertNull(index.get(1));
    Assert.assertNotNull(index.get(2));

    database.rollback();

    Assert.assertNull(database.getTransaction().getIndexChanges("idxTxAwareOneValueGetTest"));
    Assert.assertNotNull(index.get(1));
    Assert.assertNotNull(index.get(2));
  }

  @Test
  public void testRemove() {
    database.begin();
    final OIndex<?> index = database.getMetadata().getIndexManager().getIndex("idxTxAwareOneValueGetTest");
    Assert.assertTrue(index instanceof OIndexTxAwareOneValue);

    final int clusterId = database.getDefaultClusterId();
    index.put(1, new ORecordId(clusterId, 1));
    index.put(2, new ORecordId(clusterId, 2));

    database.commit();

    Assert.assertNull(database.getTransaction().getIndexChanges("idxTxAwareOneValueGetTest"));
    Assert.assertNotNull(index.get(1));
    Assert.assertNotNull(index.get(2));

    database.begin();

    index.remove(1);

    Assert.assertNotNull(database.getTransaction().getIndexChanges("idxTxAwareOneValueGetTest"));
    Assert.assertNull(index.get(1));
    Assert.assertNotNull(index.get(2));

    database.rollback();

    Assert.assertNull(database.getTransaction().getIndexChanges("idxTxAwareOneValueGetTest"));
    Assert.assertNotNull(index.get(1));
    Assert.assertNotNull(index.get(2));
  }

  @Test
  public void testRemoveAndPut() {
    database.begin();
    final OIndex<?> index = database.getMetadata().getIndexManager().getIndex("idxTxAwareOneValueGetTest");
    Assert.assertTrue(index instanceof OIndexTxAwareOneValue);

    final int clusterId = database.getDefaultClusterId();
    index.put(1, new ORecordId(clusterId, 1));
    index.put(2, new ORecordId(clusterId, 2));

    database.commit();

    Assert.assertNull(database.getTransaction().getIndexChanges("idxTxAwareOneValueGetTest"));
    Assert.assertNotNull(index.get(1));
    Assert.assertNotNull(index.get(2));

    database.begin();

    index.remove(1);
    index.put(1, new ORecordId(clusterId, 1));

    Assert.assertNotNull(database.getTransaction().getIndexChanges("idxTxAwareOneValueGetTest"));
    Assert.assertNotNull(index.get(1));
    Assert.assertNotNull(index.get(2));

    database.rollback();
  }

  @Test
  public void testMultiPut() {
    database.begin();

    final OIndex<?> index = database.getMetadata().getIndexManager().getIndex("idxTxAwareOneValueGetTest");
    Assert.assertTrue(index instanceof OIndexTxAwareOneValue);

    final int clusterId = database.getDefaultClusterId();
    index.put(1, new ORecordId(clusterId, 1));
    index.put(1, new ORecordId(clusterId, 1));

    Assert.assertNotNull(database.getTransaction().getIndexChanges("idxTxAwareOneValueGetTest"));
    Assert.assertNotNull(((OIndexTxAwareOneValue) index).get(1));
    database.commit();

    Assert.assertNotNull(((OIndexTxAwareOneValue) index).get(1));
  }

  @Test
  public void testPutAfterTransaction() {
    database.begin();

    final OIndex<?> index = database.getMetadata().getIndexManager().getIndex("idxTxAwareOneValueGetTest");
    Assert.assertTrue(index instanceof OIndexTxAwareOneValue);

    final int clusterId = database.getDefaultClusterId();
    index.put(1, new ORecordId(clusterId, 1));

    Assert.assertNotNull(database.getTransaction().getIndexChanges("idxTxAwareOneValueGetTest"));
    Assert.assertNotNull(index.get(1));
    database.commit();

    index.put(2, new ORecordId(clusterId, 2));

    Assert.assertNotNull(index.get(2));
  }

  @Test
  public void testRemoveOneWithinTransaction() {
    database.begin();

    final OIndex<?> index = database.getMetadata().getIndexManager().getIndex("idxTxAwareOneValueGetTest");
    Assert.assertTrue(index instanceof OIndexTxAwareOneValue);

    final int clusterId = database.getDefaultClusterId();
    index.put(1, new ORecordId(clusterId, 1));
    index.remove(1);

    Assert.assertNotNull(database.getTransaction().getIndexChanges("idxTxAwareOneValueGetTest"));
    Assert.assertNull(index.get(1));

    database.commit();

    Assert.assertNull(index.get(1));
  }

  @Test
  public void testRemoveAllWithinTransaction() {
    database.begin();

    final OIndex<?> index = database.getMetadata().getIndexManager().getIndex("idxTxAwareOneValueGetTest");
    Assert.assertTrue(index instanceof OIndexTxAwareOneValue);

    final int clusterId = database.getDefaultClusterId();
    index.put(1, new ORecordId(clusterId, 1));
    index.remove(1);

    Assert.assertNotNull(database.getTransaction().getIndexChanges("idxTxAwareOneValueGetTest"));
    Assert.assertNull(index.get(1));

    database.commit();

    Assert.assertNull(index.get(1));
  }

  @Test
  public void testPutAfterRemove() {
    database.begin();

    final OIndex<?> index = database.getMetadata().getIndexManager().getIndex("idxTxAwareOneValueGetTest");
    Assert.assertTrue(index instanceof OIndexTxAwareOneValue);

    final int clusterId = database.getDefaultClusterId();
    index.put(1, new ORecordId(clusterId, 1));
    index.remove(1, new ORecordId(clusterId, 1));
    index.put(1, new ORecordId(clusterId, 1));

    Assert.assertNotNull(database.getTransaction().getIndexChanges("idxTxAwareOneValueGetTest"));
    Assert.assertNotNull(index.get(1));

    database.commit();

    Assert.assertNotNull(index.get(1));
  }
}
