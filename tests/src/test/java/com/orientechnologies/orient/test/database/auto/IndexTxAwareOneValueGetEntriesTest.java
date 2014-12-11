package com.orientechnologies.orient.test.database.auto;

import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.index.OIndexCursor;
import com.orientechnologies.orient.core.index.OIndexTxAwareOneValue;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

@Test
public class IndexTxAwareOneValueGetEntriesTest extends DocumentDBBaseTest {

  @Parameters(value = "url")
  public IndexTxAwareOneValueGetEntriesTest(@Optional String url) {
    super(url);
  }

  @BeforeClass
  public void beforeClass() throws Exception {
    super.beforeClass();

    database.command(new OCommandSQL("create index idxTxAwareOneValueGetEntriesTest unique")).execute();
    database.getMetadata().getIndexManager().reload();
  }

  @AfterMethod
  public void afterMethod() throws Exception {
    database.command(new OCommandSQL("delete from index:idxTxAwareOneValueGetEntriesTest")).execute();

    super.afterMethod();
  }

  @Test
  public void testPut() {
    database.begin();
    final OIndex<?> index = database.getMetadata().getIndexManager().getIndex("idxTxAwareOneValueGetEntriesTest");
    Assert.assertTrue(index instanceof OIndexTxAwareOneValue);

    final int clusterId = database.getDefaultClusterId();
    index.put(1, new ORecordId(clusterId, 1));
    index.put(2, new ORecordId(clusterId, 2));
    database.commit();

    Assert.assertNull(database.getTransaction().getIndexChanges("idxTxAwareOneValueGetEntriesTest"));
    Set<OIdentifiable> resultOne = new HashSet<OIdentifiable>();
    OIndexCursor cursor = index.iterateEntries(Arrays.asList(1, 2), true);
    cursorToSet(cursor, resultOne);
    Assert.assertEquals(resultOne.size(), 2);

    database.begin();

    index.put(3, new ORecordId(clusterId, 3));

    Assert.assertNotNull(database.getTransaction().getIndexChanges("idxTxAwareOneValueGetEntriesTest"));
    Set<OIdentifiable> resultTwo = new HashSet<OIdentifiable>();
    cursor = index.iterateEntries(Arrays.asList(1, 2, 3), true);
    cursorToSet(cursor, resultTwo);
    Assert.assertEquals(resultTwo.size(), 3);

    database.rollback();

    Assert.assertNull(database.getTransaction().getIndexChanges("idxTxAwareOneValueGetEntriesTest"));
    Set<OIdentifiable> resultThree = new HashSet<OIdentifiable>();
    cursor = index.iterateEntries(Arrays.asList(1, 2), true);
    cursorToSet(cursor, resultThree);

    Assert.assertEquals(resultThree.size(), 2);
  }

  @Test
  public void testClear() {
    database.begin();
    final OIndex<?> index = database.getMetadata().getIndexManager().getIndex("idxTxAwareOneValueGetEntriesTest");
    Assert.assertTrue(index instanceof OIndexTxAwareOneValue);

    final int clusterId = database.getDefaultClusterId();
    index.put(1, new ORecordId(clusterId, 1));
    index.put(2, new ORecordId(clusterId, 2));
    database.commit();

    Assert.assertNull(database.getTransaction().getIndexChanges("idxTxAwareOneValueGetEntriesTest"));
    Set<OIdentifiable> resultOne = new HashSet<OIdentifiable>();
    OIndexCursor cursor = index.iterateEntries(Arrays.asList(1, 2), true);
    cursorToSet(cursor, resultOne);
    Assert.assertEquals(resultOne.size(), 2);

    database.begin();

    index.clear();

    Assert.assertNotNull(database.getTransaction().getIndexChanges("idxTxAwareOneValueGetEntriesTest"));
    Set<OIdentifiable> resultTwo = new HashSet<OIdentifiable>();
    cursor = index.iterateEntries(Arrays.asList(1, 2), true);
    cursorToSet(cursor, resultTwo);
    Assert.assertEquals(resultTwo.size(), 0);

    database.rollback();

    Assert.assertNull(database.getTransaction().getIndexChanges("idxTxAwareOneValueGetEntriesTest"));
    Set<OIdentifiable> resultThree = new HashSet<OIdentifiable>();
    cursor = index.iterateEntries(Arrays.asList(1, 2), true);
    cursorToSet(cursor, resultThree);

    Assert.assertEquals(resultThree.size(), 2);
  }

  @Test
  public void testClearAndPut() {
    database.begin();
    final OIndex<?> index = database.getMetadata().getIndexManager().getIndex("idxTxAwareOneValueGetEntriesTest");
    System.out.println("index intstanceof " + (index == null ? "null" : index.getClass()));
    Assert.assertTrue(index instanceof OIndexTxAwareOneValue);

    final int clusterId = database.getDefaultClusterId();
    index.put(1, new ORecordId(clusterId, 1));
    index.put(2, new ORecordId(clusterId, 2));
    database.commit();

    Assert.assertNull(database.getTransaction().getIndexChanges("idxTxAwareOneValueGetEntriesTest"));
    Set<OIdentifiable> resultOne = new HashSet<OIdentifiable>();
    OIndexCursor cursor = index.iterateEntries(Arrays.asList(1, 2), true);
    cursorToSet(cursor, resultOne);
    Assert.assertEquals(resultOne.size(), 2);

    database.begin();

    index.clear();
    index.put(2, new ORecordId(clusterId, 2));

    Assert.assertNotNull(database.getTransaction().getIndexChanges("idxTxAwareOneValueGetEntriesTest"));
    Set<OIdentifiable> resultTwo = new HashSet<OIdentifiable>();
    cursor = index.iterateEntries(Arrays.asList(1, 2), true);
    cursorToSet(cursor, resultTwo);
    Assert.assertEquals(resultTwo.size(), 1);

    database.rollback();

    Assert.assertNull(database.getTransaction().getIndexChanges("idxTxAwareOneValueGetEntriesTest"));
    Set<OIdentifiable> resultThree = new HashSet<OIdentifiable>();
    cursor = index.iterateEntries(Arrays.asList(1, 2), true);
    cursorToSet(cursor, resultThree);
    Assert.assertEquals(resultThree.size(), 2);
  }

  @Test
  public void testRemove() {
    database.begin();
    final OIndex<?> index = database.getMetadata().getIndexManager().getIndex("idxTxAwareOneValueGetEntriesTest");
    Assert.assertTrue(index instanceof OIndexTxAwareOneValue);

    final int clusterId = database.getDefaultClusterId();
    index.put(1, new ORecordId(clusterId, 1));
    index.put(2, new ORecordId(clusterId, 2));

    database.commit();

    Assert.assertNull(database.getTransaction().getIndexChanges("idxTxAwareOneValueGetEntriesTest"));
    Set<OIdentifiable> resultOne = new HashSet<OIdentifiable>();
    OIndexCursor cursor = index.iterateEntries(Arrays.asList(1, 2), true);
    cursorToSet(cursor, resultOne);
    Assert.assertEquals(resultOne.size(), 2);

    database.begin();

    index.remove(1);

    Assert.assertNotNull(database.getTransaction().getIndexChanges("idxTxAwareOneValueGetEntriesTest"));
    Set<OIdentifiable> resultTwo = new HashSet<OIdentifiable>();
    cursor = index.iterateEntries(Arrays.asList(1, 2), true);
    cursorToSet(cursor, resultTwo);
    Assert.assertEquals(resultTwo.size(), 1);

    database.rollback();

    Assert.assertNull(database.getTransaction().getIndexChanges("idxTxAwareOneValueGetEntriesTest"));
    Set<OIdentifiable> resultThree = new HashSet<OIdentifiable>();
    cursor = index.iterateEntries(Arrays.asList(1, 2), true);
    cursorToSet(cursor, resultThree);
    Assert.assertEquals(resultThree.size(), 2);
  }

  @Test
  public void testRemoveAndPut() {
    database.begin();
    final OIndex<?> index = database.getMetadata().getIndexManager().getIndex("idxTxAwareOneValueGetEntriesTest");
    Assert.assertTrue(index instanceof OIndexTxAwareOneValue);

    final int clusterId = database.getDefaultClusterId();
    index.put(1, new ORecordId(clusterId, 1));
    index.put(2, new ORecordId(clusterId, 2));

    database.commit();

    Assert.assertNull(database.getTransaction().getIndexChanges("idxTxAwareOneValueGetEntriesTest"));
    Set<OIdentifiable> resultOne = new HashSet<OIdentifiable>();
    OIndexCursor cursor = index.iterateEntries(Arrays.asList(1, 2), true);
    cursorToSet(cursor, resultOne);
    Assert.assertEquals(resultOne.size(), 2);

    database.begin();

    index.remove(1);
    index.put(1, new ORecordId(clusterId, 1));

    Assert.assertNotNull(database.getTransaction().getIndexChanges("idxTxAwareOneValueGetEntriesTest"));
    Set<OIdentifiable> resultTwo = new HashSet<OIdentifiable>();
    cursor = index.iterateEntries(Arrays.asList(1, 2), true);
    cursorToSet(cursor, resultTwo);
    Assert.assertEquals(resultTwo.size(), 2);

    database.rollback();
  }

  @Test
  public void testMultiPut() {
    database.begin();

    final OIndex<?> index = database.getMetadata().getIndexManager().getIndex("idxTxAwareOneValueGetEntriesTest");
    Assert.assertTrue(index instanceof OIndexTxAwareOneValue);

    final int clusterId = database.getDefaultClusterId();
    index.put(1, new ORecordId(clusterId, 1));
    index.put(1, new ORecordId(clusterId, 1));
    index.put(2, new ORecordId(clusterId, 2));

    Assert.assertNotNull(database.getTransaction().getIndexChanges("idxTxAwareOneValueGetEntriesTest"));
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
    database.begin();

    final OIndex<?> index = database.getMetadata().getIndexManager().getIndex("idxTxAwareOneValueGetEntriesTest");
    Assert.assertTrue(index instanceof OIndexTxAwareOneValue);

    final int clusterId = database.getDefaultClusterId();
    index.put(1, new ORecordId(clusterId, 1));
    index.put(2, new ORecordId(clusterId, 2));

    Assert.assertNotNull(database.getTransaction().getIndexChanges("idxTxAwareOneValueGetEntriesTest"));
    Set<OIdentifiable> result = new HashSet<OIdentifiable>();
    OIndexCursor cursor = index.iterateEntries(Arrays.asList(1, 2), true);
    cursorToSet(cursor, result);

    Assert.assertEquals(result.size(), 2);
    database.commit();

    index.put(3, new ORecordId(clusterId, 3));

    cursor = index.iterateEntries(Arrays.asList(1, 2, 3), true);
    cursorToSet(cursor, result);
    Assert.assertEquals(result.size(), 3);
  }

  @Test
  public void testRemoveOneWithinTransaction() {
    database.begin();

    final OIndex<?> index = database.getMetadata().getIndexManager().getIndex("idxTxAwareOneValueGetEntriesTest");
    Assert.assertTrue(index instanceof OIndexTxAwareOneValue);

    final int clusterId = database.getDefaultClusterId();
    index.put(1, new ORecordId(clusterId, 1));
    index.put(2, new ORecordId(clusterId, 2));

    index.remove(1);

    Assert.assertNotNull(database.getTransaction().getIndexChanges("idxTxAwareOneValueGetEntriesTest"));
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
    database.begin();

    final OIndex<?> index = database.getMetadata().getIndexManager().getIndex("idxTxAwareOneValueGetEntriesTest");
    Assert.assertTrue(index instanceof OIndexTxAwareOneValue);

    final int clusterId = database.getDefaultClusterId();
    index.put(1, new ORecordId(clusterId, 1));
    index.put(2, new ORecordId(clusterId, 2));

    index.remove(1, null);

    Assert.assertNotNull(database.getTransaction().getIndexChanges("idxTxAwareOneValueGetEntriesTest"));
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
    database.begin();

    final OIndex<?> index = database.getMetadata().getIndexManager().getIndex("idxTxAwareOneValueGetEntriesTest");
    Assert.assertTrue(index instanceof OIndexTxAwareOneValue);

    final int clusterId = database.getDefaultClusterId();
    index.put(1, new ORecordId(clusterId, 1));
    index.put(2, new ORecordId(clusterId, 2));

    index.remove(1, new ORecordId(clusterId, 1));
    index.put(1, new ORecordId(clusterId, 1));

    Assert.assertNotNull(database.getTransaction().getIndexChanges("idxTxAwareOneValueGetEntriesTest"));
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
