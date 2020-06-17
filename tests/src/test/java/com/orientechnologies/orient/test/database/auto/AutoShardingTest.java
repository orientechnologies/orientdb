/*
 * Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
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

import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.common.util.ORawPair;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sharding.auto.OAutoShardingClusterSelectionStrategy;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.core.storage.index.hashindex.local.OMurmurHash3HashFunction;
import java.util.stream.Stream;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

/** Tests Auto-Sharding indexes (Since v2.2.0). */
@Test
public class AutoShardingTest extends DocumentDBBaseTest {
  private static final int ITERATIONS = 500;
  private OClass cls;
  private OIndex idx;
  private final OMurmurHash3HashFunction<Integer> hashFunction =
      new OMurmurHash3HashFunction<>(new OIntegerSerializer());
  private int[] clusterIds;

  @Parameters(value = "url")
  public AutoShardingTest(@Optional String url) {
    super(url);
  }

  @BeforeMethod
  public void beforeMethod() throws Exception {
    super.beforeMethod();

    if (database.getMetadata().getSchema().existsClass("AutoShardingTest"))
      database.getMetadata().getSchema().dropClass("AutoShardingTest");

    cls = database.getMetadata().getSchema().createClass("AutoShardingTest");
    cls.createProperty("id", OType.INTEGER);

    idx =
        cls.createIndex(
            "testAutoSharding",
            OClass.INDEX_TYPE.NOTUNIQUE.toString(),
            null,
            null,
            "AUTOSHARDING",
            new String[] {"id"});

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
      final int selectedClusterId =
          clusterIds[((int) (Math.abs(hashFunction.hashCode(i)) % clusterIds.length))];

      @SuppressWarnings("deprecation")
      Iterable<ODocument> resultSet =
          database.command(new OCommandSQL("select from AutoShardingTest where id = ?")).execute(i);
      Assert.assertTrue(resultSet.iterator().hasNext());
      final ODocument sqlRecord = resultSet.iterator().next();
      Assert.assertEquals(sqlRecord.getIdentity().getClusterId(), selectedClusterId);
    }
  }

  @Test
  public void testDelete() {
    create();
    for (int i = 0; i < ITERATIONS; ++i) {
      @SuppressWarnings("deprecation")
      Integer deleted =
          database.command(new OCommandSQL("delete from AutoShardingTest where id = ?")).execute(i);

      Assert.assertEquals(deleted.intValue(), 2);

      long totExpected = ITERATIONS - (i + 1);
      Assert.assertEquals(idx.getInternal().size(), totExpected * 2);
      try (Stream<ORawPair<Object, ORID>> stream = idx.getInternal().stream()) {
        Assert.assertEquals(stream.map((pair) -> pair.first).distinct().count(), totExpected);
      }
    }

    Assert.assertEquals(idx.getInternal().size(), 0);
  }

  @Test
  public void testUpdate() {
    create();
    for (int i = 0; i < ITERATIONS; ++i) {
      @SuppressWarnings("deprecation")
      Integer updated =
          database
              .command(
                  new OCommandSQL(
                      "update AutoShardingTest INCREMENT id = " + ITERATIONS + " where id = ?"))
              .execute(i);

      Assert.assertEquals(updated.intValue(), 2);

      Assert.assertEquals(idx.getInternal().size(), ITERATIONS * 2);
      try (Stream<ORawPair<Object, ORID>> stream = idx.getInternal().stream()) {
        Assert.assertEquals(stream.map((pair) -> pair.first).distinct().count(), ITERATIONS);
      }
    }

    Assert.assertEquals(idx.getInternal().size(), ITERATIONS * 2);
    try (Stream<ORawPair<Object, ORID>> stream = idx.getInternal().stream()) {
      Assert.assertEquals(stream.map((pair) -> pair.first).distinct().count(), ITERATIONS);
    }
  }

  @Test
  public void testKeyCursor() {
    create();

    try (Stream<Object> stream = idx.getInternal().keyStream()) {
      Assert.assertNotNull(stream);
      Assert.assertEquals(stream.count(), ITERATIONS);
    }
  }

  public void testDrop() {
    Assert.assertTrue(cls.getClusterSelection() instanceof OAutoShardingClusterSelectionStrategy);
    database.getMetadata().getIndexManagerInternal().dropIndex(database, idx.getName());
    cls = database.getMetadata().getSchema().getClass("AutoShardingTest");
    Assert.assertFalse(cls.getClusterSelection() instanceof OAutoShardingClusterSelectionStrategy);
  }

  private void create() {
    for (int i = 0; i < ITERATIONS; ++i) {
      final int selectedClusterId =
          clusterIds[((int) (Math.abs(hashFunction.hashCode(i)) % clusterIds.length))];

      @SuppressWarnings("deprecation")
      ODocument sqlRecord =
          database
              .command(new OCommandSQL("insert into AutoShardingTest (id) values (" + i + ")"))
              .execute();
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
