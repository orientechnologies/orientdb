package com.orientechnologies.orient.core.tx;

import com.orientechnologies.orient.core.db.ODatabaseType;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.index.OIndexCursor;
import com.orientechnologies.orient.core.index.OIndexTxAwareMultiValue;
import com.orientechnologies.orient.core.index.OSimpleKeyIndexDefinition;
import com.orientechnologies.orient.core.iterator.ORecordIteratorCluster;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Created by tglman on 28/05/17.
 */
public class IndexChangesQueryTest {

  private OrientDB          orientDB;
  private ODatabaseDocument database;

  @Before
  public void before() {
    orientDB = new OrientDB("embedded:", OrientDBConfig.defaultConfig());
    orientDB.create("test", ODatabaseType.MEMORY);
    database = orientDB.open("test", "admin", "admin");
    database.getMetadata().getIndexManager()
        .createIndex("idxTxAwareMultiValueGetEntriesTest", "NOTUNIQUE", new OSimpleKeyIndexDefinition(-1, OType.INTEGER), null,
            null, null);
  }

  @After
  public void after() {
    database.close();
    orientDB.close();
  }

  private void cursorToSet(OIndexCursor cursor, Set<OIdentifiable> result) {
    result.clear();
    Map.Entry<Object, OIdentifiable> entry = cursor.nextEntry();
    while (entry != null) {
      result.add(entry.getValue());
      entry = cursor.nextEntry();
    }
  }

  @Test
  public void testMultiplePut() {
    database.begin();

    final OIndex<?> index = database.getMetadata().getIndexManager().getIndex("idxTxAwareMultiValueGetEntriesTest");
    Assert.assertTrue(index instanceof OIndexTxAwareMultiValue);

    final int clusterId = database.getDefaultClusterId();
    ODocument doc = new ODocument();
    database.save(doc);
    ODocument doc1 = new ODocument();
    database.save(doc1);
    index.put(1, doc.getIdentity());
    index.put(1, doc.getIdentity());
    index.put(2, doc1.getIdentity());

    Assert.assertNotNull(database.getTransaction().getIndexChanges("idxTxAwareMultiValueGetEntriesTest"));

    OResultSet res = database
        .query("select from index:idxTxAwareMultiValueGetEntriesTest where key in [?, ?] order by key ASC ", 1, 2);
    Assert.assertEquals(res.stream().count(), 2);

    database.commit();
  }

  @Test
  public void testClearAndPut() {

    OIdentifiable id1 = database.save(new ODocument());
    OIdentifiable id2 = database.save(new ODocument());
    OIdentifiable id3 = database.save(new ODocument());

    database.begin();
    final OIndex<?> index = database.getMetadata().getIndexManager().getIndex("idxTxAwareMultiValueGetEntriesTest");
    Assert.assertTrue(index instanceof OIndexTxAwareMultiValue);

    index.put(1, id1.getIdentity());
    index.put(1, id2.getIdentity());

    index.put(2, id3.getIdentity());

    database.commit();
    OResultSet res = database
        .query("select from index:idxTxAwareMultiValueGetEntriesTest where key in [?, ?] order by key ASC ", 1, 2);
    Assert.assertEquals(res.stream().count(), 3);
    res = database
        .query("select count(*)  as count from index:idxTxAwareMultiValueGetEntriesTest where key in [?, ?] order by key ASC ", 1,
            2);
    Assert.assertEquals((long) res.next().getProperty("count"), 3);

    database.begin();

    index.clear();
    index.put(2, id3.getIdentity());
    ODocument doc = new ODocument();
    database.save(doc);
    index.put(2, doc.getIdentity());

    res = database.query("select from index:idxTxAwareMultiValueGetEntriesTest where key in [?, ?] order by key ASC ", 1, 2);
    Assert.assertEquals(res.stream().count(), 2);
    res = database.query("select expand(rid) from index:idxTxAwareMultiValueGetEntriesTest where key = ?", 2);
    Assert.assertEquals(res.stream().count(), 2);

    res = database.query("select expand(rid) from index:idxTxAwareMultiValueGetEntriesTest where key = ?", 2);
    Assert.assertEquals(res.stream().map((aa) -> aa.getIdentity().orElse(null)).collect(Collectors.toSet()).size(), 2);

    res = database
        .query("select count(*)  as count from index:idxTxAwareMultiValueGetEntriesTest where key in [?, ?] order by key ASC ", 1,
            2);
    Assert.assertEquals((long) res.next().getProperty("count"), 2);

    database.rollback();

    Assert.assertNull(database.getTransaction().getIndexChanges("idxTxAwareMultiValueGetEntriesTest"));

    res = database.query("select from index:idxTxAwareMultiValueGetEntriesTest where key in [?, ?] order by key ASC ", 1, 2);
    Assert.assertEquals(res.stream().count(), 3);
    res = database
        .query("select count(*)  as count  from index:idxTxAwareMultiValueGetEntriesTest where key in [?, ?] order by key ASC ", 1,
            2);
    Assert.assertEquals((long) res.next().getProperty("count"), 3);
  }
}
