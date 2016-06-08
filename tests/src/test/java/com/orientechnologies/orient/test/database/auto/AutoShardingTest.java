/*
 * Copyright 2010-2012 Luca Garulli (l.garulli--at--orientechnologies.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.orientechnologies.orient.test.database.auto;

import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import com.orientechnologies.common.listener.OProgressListener;
import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.index.OIndexKeyCursor;
import com.orientechnologies.orient.core.index.hashindex.local.OMurmurHash3HashFunction;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sharding.auto.OAutoShardingClusterSelectionStrategy;
import com.orientechnologies.orient.core.sql.OCommandSQL;

/**
 * Tests Auto-Sharding indexes (Since v2.2.0).
 */
@Test
public class AutoShardingTest extends DocumentDBBaseTest {
  private static final int               ITERATIONS   = 500;
  private OClass                         cls;
  private OIndex<?>                      idx;
  private final OMurmurHash3HashFunction hashFunction = new OMurmurHash3HashFunction();
  private int[]                          clusterIds;

  @Parameters(value = "url")
  public AutoShardingTest(@Optional String url) {
    super(url);
  }

  @BeforeMethod
  public void beforeMethod() throws Exception {
    super.beforeMethod();

    hashFunction.setValueSerializer(new OIntegerSerializer());

    if (database.getMetadata().getSchema().existsClass("AutoShardingTest"))
      database.getMetadata().getSchema().dropClass("AutoShardingTest");

    cls = database.getMetadata().getSchema().createClass("AutoShardingTest");
    cls.createProperty("id", OType.INTEGER);

    idx = cls.createIndex("testAutoSharding", OClass.INDEX_TYPE.NOTUNIQUE.toString(), (OProgressListener) null, (ODocument) null,
        "AUTOSHARDING", new String[] { "id" });

    clusterIds = cls.getClusterIds();
  }

  @Test
  public void testCreate() {
    create();
  }

  @Test
  public void testQuery() {
    create();
    for (int i = 0; i < ITERATIONS; ++i) {
      final int selectedClusterId = clusterIds[((int) (Math.abs(hashFunction.hashCode(i)) % clusterIds.length))];

      Iterable<ODocument> resultSet = database.command(new OCommandSQL("select from AutoShardingTest where id = ?")).execute(i);
      Assert.assertTrue(resultSet.iterator().hasNext());
      final ODocument sqlRecord = resultSet.iterator().next();
      Assert.assertEquals(sqlRecord.getIdentity().getClusterId(), selectedClusterId);
    }
  }

  @Test
  public void testDelete() {
    create();
    for (int i = 0; i < ITERATIONS; ++i) {
      Integer deleted = database.command(new OCommandSQL("delete from AutoShardingTest where id = ?")).execute(i);

      Assert.assertEquals(deleted.intValue(), 2);

      long totExpected = ITERATIONS - (i + 1);
      Assert.assertEquals(idx.getSize(), totExpected * 2);
      Assert.assertEquals(idx.getKeySize(), totExpected);
    }

    Assert.assertEquals(idx.getSize(), 0);
    Assert.assertEquals(idx.getKeySize(), 0);
  }

  @Test
  public void testUpdate() {
    create();
    for (int i = 0; i < ITERATIONS; ++i) {
      Integer updated = database.command(new OCommandSQL("update AutoShardingTest INCREMENT id = " + ITERATIONS + " where id = ?"))
          .execute(i);

      Assert.assertEquals(updated.intValue(), 2);

      Assert.assertEquals(idx.getSize(), ITERATIONS * 2);
      Assert.assertEquals(idx.getKeySize(), ITERATIONS);
    }

    Assert.assertEquals(idx.getSize(), ITERATIONS * 2);
    Assert.assertEquals(idx.getKeySize(), ITERATIONS);
  }

  @Test
  public void testKeyCursor() {
    create();

    final OIndexKeyCursor cursor = idx.keyCursor();

    Assert.assertNotNull(cursor);
    int count = 0;
    for (Object entry = cursor.next(0); entry != null; entry = cursor.next(0)) {
      count++;
    }
    Assert.assertEquals(count, ITERATIONS);
  }

  public void testDrop() {
    Assert.assertTrue(cls.getClusterSelection() instanceof OAutoShardingClusterSelectionStrategy);
    database.getMetadata().getIndexManager().dropIndex(idx.getName());
    cls = database.getMetadata().getSchema().getClass("AutoShardingTest");
    Assert.assertFalse(cls.getClusterSelection() instanceof OAutoShardingClusterSelectionStrategy);
  }

  private void create() {
    for (int i = 0; i < ITERATIONS; ++i) {
      final int selectedClusterId = clusterIds[((int) (Math.abs(hashFunction.hashCode(i)) % clusterIds.length))];

      ODocument sqlRecord = database.command(new OCommandSQL("insert into AutoShardingTest (id) values (" + i + ")")).execute();
      Assert.assertEquals(sqlRecord.getIdentity().getClusterId(), selectedClusterId);

      ODocument apiRecord = new ODocument("AutoShardingTest").field("id", i).save();
      Assert.assertEquals(apiRecord.getIdentity().getClusterId(), selectedClusterId);
    }

    // TEST ALL CLUSTER HAVE RECORDS
    for (int clusterId : cls.getClusterIds()) {
      Assert.assertTrue(database.countClusterElements(clusterId) > 0);
    }
  }
}
