package com.orientechnologies.orient.test.database.auto;

import java.util.*;

import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.index.OIndexCursor;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.id.OClusterPositionFactory;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.index.OIndexTxAwareOneValue;
import com.orientechnologies.orient.core.sql.OCommandSQL;

@Test
public class IndexTxAwareOneValueGetValuesTest {
  private ODatabaseDocumentTx database;

  @Parameters(value = "url")
  public IndexTxAwareOneValueGetValuesTest(final String iURL) {
    this.database = new ODatabaseDocumentTx(iURL);
  }

  @BeforeClass
  public void beforeClass() {
    database.open("admin", "admin");

    database.command(new OCommandSQL("create index idxTxAwareOneValueGetValuesTest unique")).execute();
    database.close();
  }

  @BeforeMethod
  public void beforeMethod() {
    database.open("admin", "admin");
  }

  @AfterMethod
  public void afterMethod() {
    database.command(new OCommandSQL("delete from index:idxTxAwareOneValueGetValuesTest")).execute();
    database.close();
  }

  @Test
  public void testPut() {
    database.getMetadata().getIndexManager().reload();
    database.begin();
    final OIndex<?> index = database.getMetadata().getIndexManager().getIndex("idxTxAwareOneValueGetValuesTest");
    Assert.assertTrue(index instanceof OIndexTxAwareOneValue);

    final int clusterId = database.getDefaultClusterId();
    index.put(1, new ORecordId(clusterId, OClusterPositionFactory.INSTANCE.valueOf(1)));
    index.put(2, new ORecordId(clusterId, OClusterPositionFactory.INSTANCE.valueOf(2)));
    database.commit();

    Assert.assertNull(database.getTransaction().getIndexChanges("idxTxAwareOneValueGetValuesTest"));

    Set<OIdentifiable> resultOne = new HashSet<OIdentifiable>();
    OIndexCursor cursor = index.iterateEntries(Arrays.asList(1, 2), true);
    cursorToSet(cursor, resultOne);
    Assert.assertEquals(resultOne.size(), 2);

    database.begin();

    index.put(3, new ORecordId(clusterId, OClusterPositionFactory.INSTANCE.valueOf(3)));

    Assert.assertNotNull(database.getTransaction().getIndexChanges("idxTxAwareOneValueGetValuesTest"));

    Set<OIdentifiable> resultTwo = new HashSet<OIdentifiable>();
    cursor = index.iterateEntries(Arrays.asList(1, 2, 3), true);
    cursorToSet(cursor, resultTwo);
    Assert.assertEquals(resultTwo.size(), 3);

    database.rollback();

    Assert.assertNull(database.getTransaction().getIndexChanges("idxTxAwareOneValueGetValuesTest"));
    Set<OIdentifiable> resultThree = new HashSet<OIdentifiable>();
    cursor = index.iterateEntries(Arrays.asList(1, 2), true);
    cursorToSet(cursor, resultThree);
    Assert.assertEquals(resultThree.size(), 2);
  }

  @Test
  public void testClear() {
    database.getMetadata().getIndexManager().reload();
    database.begin();
    final OIndex<?> index = database.getMetadata().getIndexManager().getIndex("idxTxAwareOneValueGetValuesTest");
    Assert.assertTrue(index instanceof OIndexTxAwareOneValue);

    final int clusterId = database.getDefaultClusterId();
    index.put(1, new ORecordId(clusterId, OClusterPositionFactory.INSTANCE.valueOf(1)));
    index.put(2, new ORecordId(clusterId, OClusterPositionFactory.INSTANCE.valueOf(2)));
    database.commit();

    Assert.assertNull(database.getTransaction().getIndexChanges("idxTxAwareOneValueGetValuesTest"));
    Set<OIdentifiable> resultOne = new HashSet<OIdentifiable>();
    OIndexCursor cursor = index.iterateEntries(Arrays.asList(1, 2), true);
    cursorToSet(cursor, resultOne);
    Assert.assertEquals(resultOne.size(), 2);

    database.begin();

    index.clear();

    Assert.assertNotNull(database.getTransaction().getIndexChanges("idxTxAwareOneValueGetValuesTest"));
    cursor = index.iterateEntries(Arrays.asList(1, 2), true);
    Set<OIdentifiable> resultTwo = new HashSet<OIdentifiable>();
    cursorToSet(cursor, resultTwo);
    Assert.assertEquals(resultTwo.size(), 0);

    database.rollback();

    Assert.assertNull(database.getTransaction().getIndexChanges("idxTxAwareOneValueGetValuesTest"));
    Set<OIdentifiable> resultThree = new HashSet<OIdentifiable>();
    cursor = index.iterateEntries(Arrays.asList(1, 2), true);
    cursorToSet(cursor, resultThree);

    Assert.assertEquals(resultThree.size(), 2);
  }

  @Test
  public void testClearAndPut() {
    database.getMetadata().getIndexManager().reload();
    database.begin();
    final OIndex<?> index = database.getMetadata().getIndexManager().getIndex("idxTxAwareOneValueGetValuesTest");
    Assert.assertTrue(index instanceof OIndexTxAwareOneValue);

    final int clusterId = database.getDefaultClusterId();
    index.put(1, new ORecordId(clusterId, OClusterPositionFactory.INSTANCE.valueOf(1)));
    index.put(2, new ORecordId(clusterId, OClusterPositionFactory.INSTANCE.valueOf(2)));
    database.commit();

    Assert.assertNull(database.getTransaction().getIndexChanges("idxTxAwareOneValueGetValuesTest"));
    Set<OIdentifiable> resultOne = new HashSet<OIdentifiable>();
    OIndexCursor cursor = index.iterateEntries(Arrays.asList(1, 2), true);
    cursorToSet(cursor, resultOne);

    Assert.assertEquals(resultOne.size(), 2);

    database.begin();

    index.clear();
    index.put(2, new ORecordId(clusterId, OClusterPositionFactory.INSTANCE.valueOf(2)));

    Assert.assertNotNull(database.getTransaction().getIndexChanges("idxTxAwareOneValueGetValuesTest"));
    Set<OIdentifiable> resultTwo = new HashSet<OIdentifiable>();
    cursor = index.iterateEntries(Arrays.asList(1, 2), true);
    cursorToSet(cursor, resultTwo);

    Assert.assertEquals(resultTwo.size(), 1);

    database.rollback();

    Assert.assertNull(database.getTransaction().getIndexChanges("idxTxAwareOneValueGetValuesTest"));

    Set<OIdentifiable> resultThree = new HashSet<OIdentifiable>();
    cursor = index.iterateEntries(Arrays.asList(1, 2), true);
    cursorToSet(cursor, resultThree);

    Assert.assertEquals(resultThree.size(), 2);
  }

  @Test
  public void testRemove() {
    database.getMetadata().getIndexManager().reload();
    database.begin();
    final OIndex<?> index = database.getMetadata().getIndexManager().getIndex("idxTxAwareOneValueGetValuesTest");
    Assert.assertTrue(index instanceof OIndexTxAwareOneValue);

    final int clusterId = database.getDefaultClusterId();
    index.put(1, new ORecordId(clusterId, OClusterPositionFactory.INSTANCE.valueOf(1)));
    index.put(2, new ORecordId(clusterId, OClusterPositionFactory.INSTANCE.valueOf(2)));

    database.commit();

    Assert.assertNull(database.getTransaction().getIndexChanges("idxTxAwareOneValueGetValuesTest"));
    OIndexCursor cursor = index.iterateEntries(Arrays.asList(1, 2), true);
    Set<OIdentifiable> resultOne = new HashSet<OIdentifiable>();
    cursorToSet(cursor, resultOne);
    Assert.assertEquals(resultOne.size(), 2);

    database.begin();

    index.remove(1);

    Assert.assertNotNull(database.getTransaction().getIndexChanges("idxTxAwareOneValueGetValuesTest"));
    cursor = index.iterateEntries(Arrays.asList(1, 2), true);
    Set<OIdentifiable> resultTwo = new HashSet<OIdentifiable>();
    cursorToSet(cursor, resultTwo);
    Assert.assertEquals(resultTwo.size(), 1);

    database.rollback();

    Assert.assertNull(database.getTransaction().getIndexChanges("idxTxAwareOneValueGetValuesTest"));
    Set<OIdentifiable> resultThree = new HashSet<OIdentifiable>();
    cursor = index.iterateEntries(Arrays.asList(1, 2), true);
    cursorToSet(cursor, resultThree);
    Assert.assertEquals(resultThree.size(), 2);
  }

  @Test
  public void testRemoveAndPut() {
    database.getMetadata().getIndexManager().reload();
    database.begin();
    final OIndex<?> index = database.getMetadata().getIndexManager().getIndex("idxTxAwareOneValueGetValuesTest");
    Assert.assertTrue(index instanceof OIndexTxAwareOneValue);

    final int clusterId = database.getDefaultClusterId();
    index.put(1, new ORecordId(clusterId, OClusterPositionFactory.INSTANCE.valueOf(1)));
    index.put(2, new ORecordId(clusterId, OClusterPositionFactory.INSTANCE.valueOf(2)));

    database.commit();

    Assert.assertNull(database.getTransaction().getIndexChanges("idxTxAwareOneValueGetValuesTest"));
    Set<OIdentifiable> resultOne = new HashSet<OIdentifiable>();
    OIndexCursor cursor = index.iterateEntries(Arrays.asList(1, 2), true);
    cursorToSet(cursor, resultOne);
    Assert.assertEquals(resultOne.size(), 2);

    database.begin();

    index.remove(1);
    index.put(1, new ORecordId(clusterId, OClusterPositionFactory.INSTANCE.valueOf(1)));

    Assert.assertNotNull(database.getTransaction().getIndexChanges("idxTxAwareOneValueGetValuesTest"));
    Set<OIdentifiable> resultTwo = new HashSet<OIdentifiable>();
    cursor = index.iterateEntries(Arrays.asList(1, 2), true);
    cursorToSet(cursor, resultTwo);
    Assert.assertEquals(resultTwo.size(), 2);

    database.rollback();
  }

  @Test
  public void testMultiPut() {
    database.getMetadata().getIndexManager().reload();
    database.begin();

    final OIndex<?> index = database.getMetadata().getIndexManager().getIndex("idxTxAwareOneValueGetValuesTest");
    Assert.assertTrue(index instanceof OIndexTxAwareOneValue);

    final int clusterId = database.getDefaultClusterId();
    index.put(1, new ORecordId(clusterId, OClusterPositionFactory.INSTANCE.valueOf(1)));
    index.put(1, new ORecordId(clusterId, OClusterPositionFactory.INSTANCE.valueOf(1)));
    index.put(2, new ORecordId(clusterId, OClusterPositionFactory.INSTANCE.valueOf(2)));

    Assert.assertNotNull(database.getTransaction().getIndexChanges("idxTxAwareOneValueGetValuesTest"));
    Set<OIdentifiable> result = new HashSet<OIdentifiable>();
    OIndexCursor cursor = index.iterateEntries(Arrays.asList(1, 2), true);
    cursorToSet(cursor, result);

    Assert.assertEquals(result.size(), 2);

    database.commit();

    cursor = index.iterateEntries(Arrays.asList(1, 2), true);
    cursorToSet(cursor, result);
    Assert.assertEquals(result.size(), 2);
  }

  @Test
  public void testPutAfterTransaction() {
    database.getMetadata().getIndexManager().reload();
    database.begin();

    final OIndex<?> index = database.getMetadata().getIndexManager().getIndex("idxTxAwareOneValueGetValuesTest");
    Assert.assertTrue(index instanceof OIndexTxAwareOneValue);

    final int clusterId = database.getDefaultClusterId();
    index.put(1, new ORecordId(clusterId, OClusterPositionFactory.INSTANCE.valueOf(1)));
    index.put(2, new ORecordId(clusterId, OClusterPositionFactory.INSTANCE.valueOf(2)));

    Assert.assertNotNull(database.getTransaction().getIndexChanges("idxTxAwareOneValueGetValuesTest"));

    Set<OIdentifiable> result = new HashSet<OIdentifiable>();
    OIndexCursor cursor = index.iterateEntries(Arrays.asList(1, 2), true);
    cursorToSet(cursor, result);

    Assert.assertEquals(result.size(), 2);
    database.commit();

    index.put(3, new ORecordId(clusterId, OClusterPositionFactory.INSTANCE.valueOf(3)));

    cursor = index.iterateEntries(Arrays.asList(1, 2, 3), true);
    cursorToSet(cursor, result);

    Assert.assertEquals(result.size(), 3);
  }

  @Test
  public void testRemoveOneWithinTransaction() {
    database.getMetadata().getIndexManager().reload();
    database.begin();

    final OIndex<?> index = database.getMetadata().getIndexManager().getIndex("idxTxAwareOneValueGetValuesTest");
    Assert.assertTrue(index instanceof OIndexTxAwareOneValue);

    final int clusterId = database.getDefaultClusterId();
    index.put(1, new ORecordId(clusterId, OClusterPositionFactory.INSTANCE.valueOf(1)));
    index.put(2, new ORecordId(clusterId, OClusterPositionFactory.INSTANCE.valueOf(2)));

    index.remove(1);

    Assert.assertNotNull(database.getTransaction().getIndexChanges("idxTxAwareOneValueGetValuesTest"));
    Set<OIdentifiable> result = new HashSet<OIdentifiable>();
    OIndexCursor cursor = index.iterateEntries(Arrays.asList(1, 2), true);
    cursorToSet(cursor, result);
    Assert.assertEquals(result.size(), 1);

    database.commit();

    cursor = index.iterateEntries(Arrays.asList(1, 2), true);
    cursorToSet(cursor, result);
    Assert.assertEquals(result.size(), 1);
  }

  @Test
  public void testRemoveAllWithinTransaction() {
    database.getMetadata().getIndexManager().reload();
    database.begin();

    final OIndex<?> index = database.getMetadata().getIndexManager().getIndex("idxTxAwareOneValueGetValuesTest");
    Assert.assertTrue(index instanceof OIndexTxAwareOneValue);

    final int clusterId = database.getDefaultClusterId();
    index.put(1, new ORecordId(clusterId, OClusterPositionFactory.INSTANCE.valueOf(1)));
    index.put(2, new ORecordId(clusterId, OClusterPositionFactory.INSTANCE.valueOf(2)));

    index.remove(1, null);

    Assert.assertNotNull(database.getTransaction().getIndexChanges("idxTxAwareOneValueGetValuesTest"));
    Set<OIdentifiable> result = new HashSet<OIdentifiable>();
    OIndexCursor cursor = index.iterateEntries(Arrays.asList(1, 2), true);
    cursorToSet(cursor, result);
    Assert.assertEquals(result.size(), 1);

    database.commit();

    cursor = index.iterateEntries(Arrays.asList(1, 2), true);
    cursorToSet(cursor, result);
    Assert.assertEquals(result.size(), 1);
  }

  @Test
  public void testPutAfterRemove() {
    database.getMetadata().getIndexManager().reload();
    database.begin();

    final OIndex<?> index = database.getMetadata().getIndexManager().getIndex("idxTxAwareOneValueGetValuesTest");
    Assert.assertTrue(index instanceof OIndexTxAwareOneValue);

    final int clusterId = database.getDefaultClusterId();
    index.put(1, new ORecordId(clusterId, OClusterPositionFactory.INSTANCE.valueOf(1)));
    index.put(2, new ORecordId(clusterId, OClusterPositionFactory.INSTANCE.valueOf(2)));

    index.remove(1, new ORecordId(clusterId, OClusterPositionFactory.INSTANCE.valueOf(1)));
    index.put(1, new ORecordId(clusterId, OClusterPositionFactory.INSTANCE.valueOf(1)));

    Assert.assertNotNull(database.getTransaction().getIndexChanges("idxTxAwareOneValueGetValuesTest"));
    Set<OIdentifiable> result = new HashSet<OIdentifiable>();
    OIndexCursor cursor = index.iterateEntries(Arrays.asList(1, 2), true);
    cursorToSet(cursor, result);

    Assert.assertEquals(result.size(), 2);

    database.commit();

    cursor = index.iterateEntries(Arrays.asList(1, 2), true);
    cursorToSet(cursor, result);
    Assert.assertEquals(result.size(), 2);
  }

  private void cursorToSet(OIndexCursor cursor, Set<OIdentifiable> result) {
    result.clear();
    Map.Entry<Object, OIdentifiable> entry = cursor.nextEntry();
    while (entry != null) {
      result.add(entry.getValue());
      entry = cursor.nextEntry();
    }
  }
}
