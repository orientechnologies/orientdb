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

import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import java.util.List;
import java.util.Set;
import org.testng.Assert;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

@Test(groups = "sql-select")
public class GEOTest extends DocumentDBBaseTest {

  @Parameters(value = "url")
  public GEOTest(@Optional String url) {
    super(url);
  }

  @Test
  public void geoSchema() {
    final OClass mapPointClass = database.getMetadata().getSchema().createClass("MapPoint");
    mapPointClass.createProperty("x", OType.DOUBLE).createIndex(OClass.INDEX_TYPE.NOTUNIQUE);
    mapPointClass.createProperty("y", OType.DOUBLE).createIndex(OClass.INDEX_TYPE.NOTUNIQUE);

    final Set<OIndex> xIndexes =
        database.getMetadata().getSchema().getClass("MapPoint").getProperty("x").getIndexes();
    Assert.assertEquals(xIndexes.size(), 1);

    final Set<OIndex> yIndexes =
        database.getMetadata().getSchema().getClass("MapPoint").getProperty("y").getIndexes();
    Assert.assertEquals(yIndexes.size(), 1);
  }

  @Test(dependsOnMethods = "geoSchema")
  public void checkGeoIndexes() {
    final Set<OIndex> xIndexes =
        database.getMetadata().getSchema().getClass("MapPoint").getProperty("x").getIndexes();
    Assert.assertEquals(xIndexes.size(), 1);

    final Set<OIndex> yIndexDefinitions =
        database.getMetadata().getSchema().getClass("MapPoint").getProperty("y").getIndexes();
    Assert.assertEquals(yIndexDefinitions.size(), 1);
  }

  @Test(dependsOnMethods = "checkGeoIndexes")
  public void queryCreatePoints() {
    ODocument point = new ODocument();

    for (int i = 0; i < 10000; ++i) {
      point.reset();
      point.setClassName("MapPoint");

      point.field("x", (52.20472d + i / 100d));
      point.field("y", (0.14056d + i / 100d));

      point.save();
    }
  }

  @Test(dependsOnMethods = "queryCreatePoints")
  public void queryDistance() {
    Assert.assertEquals(database.countClass("MapPoint"), 10000);

    List<ODocument> result =
        database
            .command(
                new OSQLSynchQuery<ODocument>(
                    "select from MapPoint where distance(x, y,52.20472, 0.14056 ) <= 30"))
            .execute();

    Assert.assertTrue(result.size() != 0);

    for (ODocument d : result) {
      Assert.assertEquals(d.getClassName(), "MapPoint");
      Assert.assertEquals(ORecordInternal.getRecordType(d), ODocument.RECORD_TYPE);
    }
  }

  @Test(dependsOnMethods = "queryCreatePoints")
  public void queryDistanceOrdered() {
    Assert.assertEquals(database.countClass("MapPoint"), 10000);

    // MAKE THE FIRST RECORD DIRTY TO TEST IF DISTANCE JUMP IT
    List<ODocument> result =
        database.command(new OSQLSynchQuery<ODocument>("select from MapPoint limit 1")).execute();
    try {
      result.get(0).field("x", "--wrong--");
      Assert.assertTrue(false);
    } catch (NumberFormatException e) {
      Assert.assertTrue(true);
    }

    result.get(0).save();

    result =
        database
            .command(
                new OSQLSynchQuery<ODocument>(
                    "select distance(x, y,52.20472, 0.14056 ) as distance from MapPoint order by distance desc"))
            .execute();

    Assert.assertTrue(result.size() != 0);

    Double lastDistance = null;
    for (ODocument d : result) {
      if (lastDistance != null && d.field("distance") != null)
        Assert.assertTrue(((Double) d.field("distance")).compareTo(lastDistance) <= 0);
      lastDistance = d.field("distance");
    }
  }
}
