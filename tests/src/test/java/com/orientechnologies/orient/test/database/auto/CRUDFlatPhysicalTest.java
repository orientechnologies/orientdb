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

import java.util.HashSet;
import java.util.Set;

import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import com.orientechnologies.orient.core.iterator.ORecordIteratorCluster;
import com.orientechnologies.orient.core.record.impl.ORecordFlat;

@Test(groups = { "crud", "record-csv" }, sequential = true)
public class CRUDFlatPhysicalTest extends DocumentDBBaseTest {
  protected static final int  TOT_RECORDS  = 100;
  private static final String CLUSTER_NAME = "binary";
  protected long              startRecordNumber;
  private ORecordFlat         record;

  @Parameters(value = "url")
  public CRUDFlatPhysicalTest(@Optional String url) {
    super(url);
  }

  @BeforeClass
  @Override
  public void beforeClass() throws Exception {
    super.beforeClass();
    record = new ORecordFlat();
  }

  public void createRaw() {
    if (database.getClusterIdByName(CLUSTER_NAME) < 0)
      database.addCluster(CLUSTER_NAME);

    startRecordNumber = database.countClusterElements(CLUSTER_NAME);

    for (long i = startRecordNumber; i < startRecordNumber + TOT_RECORDS; ++i) {
      record.reset();
      record.value(i + "-binary test").save(CLUSTER_NAME);
    }

    Assert.assertEquals(database.countClusterElements(CLUSTER_NAME) - startRecordNumber, TOT_RECORDS);
  }

  @Test(dependsOnMethods = "createRaw")
  public void readRawWithExpressiveForwardIterator() {
    String[] fields;

    Set<Integer> ids = new HashSet<Integer>(TOT_RECORDS);
    for (int i = 0; i < TOT_RECORDS; i++)
      ids.add(i);

    for (ORecordFlat rec : new ORecordIteratorCluster<ORecordFlat>(database, database, database.getClusterIdByName(CLUSTER_NAME))) {
      fields = rec.value().split("-");

      int i = Integer.parseInt(fields[0]);
      Assert.assertTrue(ids.remove(i));
    }

    Assert.assertTrue(ids.isEmpty());
  }

  @Test(dependsOnMethods = "readRawWithExpressiveForwardIterator")
  public void updateRaw() {
    String[] fields;

    for (ORecordFlat rec : new ORecordIteratorCluster<ORecordFlat>(database, database, database.getClusterIdByName(CLUSTER_NAME))) {
      fields = rec.value().split("-");
      int i = Integer.parseInt(fields[0]);
      if (i % 2 == 0) {
        rec.value(rec.value() + "+");
        rec.save();
      }
    }
  }

  @Test(dependsOnMethods = "updateRaw")
  public void testUpdateRaw() {
    String[] fields;

    Set<Integer> ids = new HashSet<Integer>(TOT_RECORDS);
    for (int i = 0; i < TOT_RECORDS; i++)
      ids.add(i);

    for (ORecordFlat rec : new ORecordIteratorCluster<ORecordFlat>(database, database, database.getClusterIdByName(CLUSTER_NAME))) {
      fields = rec.value().split("-");

      int i = Integer.parseInt(fields[0]);
      Assert.assertTrue(ids.remove(i));

      if (i % 2 == 0)
        Assert.assertTrue(fields[1].endsWith("+"));
      else
        Assert.assertFalse(fields[1].endsWith("+"));
    }

    Assert.assertTrue(ids.isEmpty());
  }
}
