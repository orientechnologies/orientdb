package com.orientechnologies.orient.test.database.auto;

import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.index.OIndexTxAwareMultiValue;
import com.orientechnologies.orient.core.iterator.ORecordIteratorCluster;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

@Test
public class IndexTxAwareMultiValueGetTest extends DocumentDBBaseTest {
  @Parameters(value = "url")
  public IndexTxAwareMultiValueGetTest(@Optional String url) {
    super(url);
  }

  @BeforeClass
  public void beforeClass() throws Exception {
    super.beforeClass();

    database.command(new OCommandSQL("create index idxTxAwareMultiValueGetTest notunique INTEGER")).execute();
  }

  @AfterMethod
  public void afterMethod() throws Exception {
    database.command(new OCommandSQL("delete from index:idxTxAwareMultiValueGetTest")).execute();

    super.afterMethod();
  }

  @Test
  public void testPut() {
    database.getMetadata().getIndexManager().reload();
    database.begin();
    final OIndex<?> index = database.getMetadata().getIndexManager().getIndex("idxTxAwareMultiValueGetTest");
    Assert.assertTrue(index instanceof OIndexTxAwareMultiValue);

    final int clusterId = database.getDefaultClusterId();
    final List<Long> positions = getValidPositions(clusterId);

    index.put(1, new ORecordId(clusterId, positions.get(0)));
    index.put(1, new ORecordId(clusterId, positions.get(1)));

    index.put(2, new ORecordId(clusterId, positions.get(2)));
    database.commit();

    Assert.assertNull(database.getTransaction().getIndexChanges("idxTxAwareMultiValueGetTest"));
    Assert.assertEquals(((OIndexTxAwareMultiValue) index).get(1).size(), 2);
    Assert.assertEquals(((OIndexTxAwareMultiValue) index).get(2).size(), 1);

    database.begin();

    index.put(2, new ORecordId(clusterId, positions.get(3)));

    Assert.assertNotNull(database.getTransaction().getIndexChanges("idxTxAwareMultiValueGetTest"));
    Assert.assertEquals(((OIndexTxAwareMultiValue) index).get(2).size(), 2);

    database.rollback();

    Assert.assertNull(database.getTransaction().getIndexChanges("idxTxAwareMultiValueGetTest"));
    Assert.assertEquals(((OIndexTxAwareMultiValue) index).get(1).size(), 2);
    Assert.assertEquals(((OIndexTxAwareMultiValue) index).get(2).size(), 1);
  }

  @Test
  public void testClear() {
    database.getMetadata().getIndexManager().reload();
    database.begin();
    final OIndex<?> index = database.getMetadata().getIndexManager().getIndex("idxTxAwareMultiValueGetTest");
    Assert.assertTrue(index instanceof OIndexTxAwareMultiValue);

    final int clusterId = database.getDefaultClusterId();
    final List<Long> positions = getValidPositions(clusterId);

    index.put(1, new ORecordId(clusterId, positions.get(0)));
    index.put(1, new ORecordId(clusterId, positions.get(1)));

    index.put(2, new ORecordId(clusterId, positions.get(2)));
    database.commit();

    Assert.assertNull(database.getTransaction().getIndexChanges("idxTxAwareMultiValueGetTest"));
    Assert.assertEquals(((OIndexTxAwareMultiValue) index).get(1).size(), 2);
    Assert.assertEquals(((OIndexTxAwareMultiValue) index).get(2).size(), 1);

    database.begin();

    index.clear();

    Assert.assertNotNull(database.getTransaction().getIndexChanges("idxTxAwareMultiValueGetTest"));
    Assert.assertNull(((OIndexTxAwareMultiValue) index).get(1));
    Assert.assertNull(((OIndexTxAwareMultiValue) index).get(2));

    database.rollback();

    Assert.assertNull(database.getTransaction().getIndexChanges("idxTxAwareMultiValueGetTest"));
    Assert.assertEquals(((OIndexTxAwareMultiValue) index).get(1).size(), 2);
    Assert.assertEquals(((OIndexTxAwareMultiValue) index).get(2).size(), 1);
  }

  @Test
  public void testClearAndPut() {
    database.getMetadata().getIndexManager().reload();
    database.begin();
    final OIndex<?> index = database.getMetadata().getIndexManager().getIndex("idxTxAwareMultiValueGetTest");
    Assert.assertTrue(index instanceof OIndexTxAwareMultiValue);

    final int clusterId = database.getDefaultClusterId();
    final List<Long> positions = getValidPositions(clusterId);

    index.put(1, new ORecordId(clusterId, positions.get(0)));
    index.put(1, new ORecordId(clusterId, positions.get(1)));

    index.put(2, new ORecordId(clusterId, positions.get(2)));
    database.commit();

    Assert.assertNull(database.getTransaction().getIndexChanges("idxTxAwareMultiValueGetTest"));
    Assert.assertEquals(((OIndexTxAwareMultiValue) index).get(1).size(), 2);
    Assert.assertEquals(((OIndexTxAwareMultiValue) index).get(2).size(), 1);

    database.begin();

    index.clear();
    index.put(2, new ORecordId(clusterId, positions.get(3)));

    Assert.assertNotNull(database.getTransaction().getIndexChanges("idxTxAwareMultiValueGetTest"));
    Assert.assertNull(index.get(1));
    Assert.assertEquals(((OIndexTxAwareMultiValue) index).get(2).size(), 1);

    database.rollback();

    Assert.assertNull(database.getTransaction().getIndexChanges("idxTxAwareMultiValueGetTest"));
    Assert.assertEquals(((OIndexTxAwareMultiValue) index).get(1).size(), 2);
    Assert.assertEquals(((OIndexTxAwareMultiValue) index).get(2).size(), 1);
  }

  @Test
  public void testRemove() {
    database.getMetadata().getIndexManager().reload();
    database.begin();
    final OIndex<?> index = database.getMetadata().getIndexManager().getIndex("idxTxAwareMultiValueGetTest");
    Assert.assertTrue(index instanceof OIndexTxAwareMultiValue);

    final int clusterId = database.getDefaultClusterId();
    final List<Long> positions = getValidPositions(clusterId);

    index.put(1, new ORecordId(clusterId, positions.get(0)));
    index.put(1, new ORecordId(clusterId, positions.get(1)));

    index.put(2, new ORecordId(clusterId, positions.get(2)));
    database.commit();

    Assert.assertNull(database.getTransaction().getIndexChanges("idxTxAwareMultiValueGetTest"));
    Assert.assertEquals(((OIndexTxAwareMultiValue) index).get(1).size(), 2);
    Assert.assertEquals(((OIndexTxAwareMultiValue) index).get(2).size(), 1);

    database.begin();

    index.remove(1);

    Assert.assertNotNull(database.getTransaction().getIndexChanges("idxTxAwareMultiValueGetTest"));
    Assert.assertNull(((OIndexTxAwareMultiValue) index).get(1));
    Assert.assertEquals(((OIndexTxAwareMultiValue) index).get(2).size(), 1);

    database.rollback();

    Assert.assertNull(database.getTransaction().getIndexChanges("idxTxAwareMultiValueGetTest"));
    Assert.assertEquals(((OIndexTxAwareMultiValue) index).get(1).size(), 2);
    Assert.assertEquals(((OIndexTxAwareMultiValue) index).get(2).size(), 1);
  }

  @Test
  public void testRemoveOne() {
    database.getMetadata().getIndexManager().reload();
    database.begin();
    final OIndex<?> index = database.getMetadata().getIndexManager().getIndex("idxTxAwareMultiValueGetTest");
    Assert.assertTrue(index instanceof OIndexTxAwareMultiValue);

    final int clusterId = database.getDefaultClusterId();
    final List<Long> positions = getValidPositions(clusterId);

    final ORecordId firstRecordId = new ORecordId(clusterId, positions.get(0));
    index.put(1, firstRecordId);
    index.put(1, new ORecordId(clusterId, positions.get(1)));

    index.put(2, new ORecordId(clusterId, positions.get(2)));
    database.commit();

    Assert.assertNull(database.getTransaction().getIndexChanges("idxTxAwareMultiValueGetTest"));
    Assert.assertEquals(((OIndexTxAwareMultiValue) index).get(1).size(), 2);
    Assert.assertEquals(((OIndexTxAwareMultiValue) index).get(2).size(), 1);

    database.begin();

    index.remove(1, firstRecordId);

    Assert.assertNotNull(database.getTransaction().getIndexChanges("idxTxAwareMultiValueGetTest"));
    Assert.assertEquals(((OIndexTxAwareMultiValue) index).get(1).size(), 1);
    Assert.assertEquals(((OIndexTxAwareMultiValue) index).get(2).size(), 1);

    database.rollback();

    Assert.assertNull(database.getTransaction().getIndexChanges("idxTxAwareMultiValueGetTest"));
    Assert.assertEquals(((OIndexTxAwareMultiValue) index).get(1).size(), 2);
    Assert.assertEquals(((OIndexTxAwareMultiValue) index).get(2).size(), 1);
  }

  @Test
  public void testMultiPut() {
    database.getMetadata().getIndexManager().reload();
    database.begin();

    final OIndex<?> index = database.getMetadata().getIndexManager().getIndex("idxTxAwareMultiValueGetTest");
    Assert.assertTrue(index instanceof OIndexTxAwareMultiValue);

    final int clusterId = database.getDefaultClusterId();
    index.put(1, new ORecordId(clusterId, 1));
    Assert.assertEquals(((OIndexTxAwareMultiValue) index).get(1).size(), 1);

    index.put(1, new ORecordId(clusterId, 1));

    Assert.assertNotNull(database.getTransaction().getIndexChanges("idxTxAwareMultiValueGetTest"));
    Assert.assertEquals(((OIndexTxAwareMultiValue) index).get(1).size(), 1);
    database.commit();

    Assert.assertEquals(((OIndexTxAwareMultiValue) index).get(1).size(), 1);
  }

  @Test
  public void testPutAfterTransaction() {
    database.getMetadata().getIndexManager().reload();
    database.begin();

    final OIndex<?> index = database.getMetadata().getIndexManager().getIndex("idxTxAwareMultiValueGetTest");
    Assert.assertTrue(index instanceof OIndexTxAwareMultiValue);

    final int clusterId = database.getDefaultClusterId();
    List<Long> positions = getValidPositions(clusterId);
    index.put(1, new ORecordId(clusterId, positions.get(1)));

    Assert.assertNotNull(database.getTransaction().getIndexChanges("idxTxAwareMultiValueGetTest"));
    Collection<?> resultOne = ((OIndexTxAwareMultiValue) index).get(1);
    Assert.assertEquals(resultOne.size(), 1);
    database.commit();

    index.put(1, new ORecordId(clusterId + 1, positions.get(1)));

    resultOne = ((OIndexTxAwareMultiValue) index).get(1);
    Assert.assertEquals(resultOne.size(), 2);
  }

  @Test
  public void testRemoveOneWithinTransaction() {
    database.getMetadata().getIndexManager().reload();
    database.begin();

    final OIndex<?> index = database.getMetadata().getIndexManager().getIndex("idxTxAwareMultiValueGetTest");
    Assert.assertTrue(index instanceof OIndexTxAwareMultiValue);

    final int clusterId = database.getDefaultClusterId();
    index.put(1, new ORecordId(clusterId, 1));
    index.remove(1, new ORecordId(clusterId, 1));

    Assert.assertNotNull(database.getTransaction().getIndexChanges("idxTxAwareMultiValueGetTest"));
    Collection<?> result = ((OIndexTxAwareMultiValue) index).get(1);
    Assert.assertNull(result);

    database.commit();

    result = ((OIndexTxAwareMultiValue) index).get(1);
    Assert.assertEquals(result.size(), 0);
  }

  @Test
  public void testRemoveAllWithinTransaction() {
    database.getMetadata().getIndexManager().reload();
    database.begin();

    final OIndex<?> index = database.getMetadata().getIndexManager().getIndex("idxTxAwareMultiValueGetTest");
    Assert.assertTrue(index instanceof OIndexTxAwareMultiValue);

    final int clusterId = database.getDefaultClusterId();
    index.put(1, new ORecordId(clusterId, 1));
    index.remove(1, null);

    Assert.assertNotNull(database.getTransaction().getIndexChanges("idxTxAwareMultiValueGetTest"));
    Collection<?> result = ((OIndexTxAwareMultiValue) index).get(1);
    Assert.assertNull(result);

    database.commit();

    result = ((OIndexTxAwareMultiValue) index).get(1);
    Assert.assertEquals(result.size(), 0);
  }

  @Test
  public void testPutAfterRemove() {
    database.getMetadata().getIndexManager().reload();
    database.begin();

    final OIndex<?> index = database.getMetadata().getIndexManager().getIndex("idxTxAwareMultiValueGetTest");
    Assert.assertTrue(index instanceof OIndexTxAwareMultiValue);

    final int clusterId = database.getDefaultClusterId();
    List<Long> positions = getValidPositions(clusterId);

    index.put(1, new ORecordId(clusterId, positions.get(1)));
    index.remove(1, new ORecordId(clusterId, positions.get(1)));
    index.put(1, new ORecordId(clusterId, positions.get(1)));

    Assert.assertNotNull(database.getTransaction().getIndexChanges("idxTxAwareMultiValueGetTest"));
    Collection<?> result = ((OIndexTxAwareMultiValue) index).get(1);
    Assert.assertEquals(result.size(), 1);

    database.commit();

    result = ((OIndexTxAwareMultiValue) index).get(1);
    Assert.assertEquals(result.size(), 1);
  }

  private List<Long> getValidPositions(int clusterId) {
    final List<Long> positions = new ArrayList<Long>();

    final ORecordIteratorCluster<?> iteratorCluster = database.browseCluster(database.getClusterNameById(clusterId));

    for (int i = 0; i < 4; i++) {
      iteratorCluster.hasNext();
      ORecord doc = iteratorCluster.next();
      positions.add(doc.getIdentity().getClusterPosition());
    }
    return positions;
  }

}
