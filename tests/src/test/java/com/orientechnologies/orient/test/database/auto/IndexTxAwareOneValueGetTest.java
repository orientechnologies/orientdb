package com.orientechnologies.orient.test.database.auto;

import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.index.OIndexTxAwareOneValue;
import com.orientechnologies.orient.core.sql.OCommandSQL;

@Test
public class IndexTxAwareOneValueGetTest extends DocumentDBBaseTest {

  @Parameters(value = "url")
  public IndexTxAwareOneValueGetTest(@Optional String url) {
    super(url);
  }

  @BeforeClass
  public void beforeClass() throws Exception {
    super.beforeClass();

    database.command(new OCommandSQL("create index idxTxAwareOneValueGetTest unique")).execute();
  }

  @AfterMethod
  public void afterMethod() throws Exception {
    database.command(new OCommandSQL("delete from index:idxTxAwareOneValueGetTest")).execute();

    super.afterMethod();
  }

  @Test
  public void testPut() {
    database.getMetadata().getIndexManager().reload();
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
    database.getMetadata().getIndexManager().reload();
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
    database.getMetadata().getIndexManager().reload();
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
    database.getMetadata().getIndexManager().reload();
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
    database.getMetadata().getIndexManager().reload();
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
    database.getMetadata().getIndexManager().reload();
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
    database.getMetadata().getIndexManager().reload();
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
    database.getMetadata().getIndexManager().reload();
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
    database.getMetadata().getIndexManager().reload();
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
    database.getMetadata().getIndexManager().reload();
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
