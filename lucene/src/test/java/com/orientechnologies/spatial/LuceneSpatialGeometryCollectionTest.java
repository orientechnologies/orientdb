/**
 * Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
 *
 * <p>Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * <p>For more information: http://www.orientdb.com
 */
package com.orientechnologies.spatial;

import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import java.util.Arrays;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/** Created by Dileep R */
public class LuceneSpatialGeometryCollectionTest extends BaseSpatialLuceneTest {

  @Before
  public void init() {
    db.command("create class test").close();
    db.command("create property test.name STRING").close();
    db.command("create property test.geometry EMBEDDED OGeometryCollection").close();

    db.command("create index test.geometry on test (geometry) SPATIAL engine lucene").close();
  }

  @Test
  public void testGeoCollectionOutsideTx() {
    ODocument test1 = new ODocument("test");
    test1.field("name", "test1");
    ODocument geometry = new ODocument("OGeometryCollection");
    ODocument point = new ODocument("OPoint");
    point.field("coordinates", Arrays.asList(1.0, 2.0));
    ODocument polygon = new ODocument("OPolygon");
    polygon.field(
        "coordinates",
        Arrays.asList(
            Arrays.asList(
                Arrays.asList(0.0, 0.0),
                Arrays.asList(10.0, 0.0),
                Arrays.asList(10.0, 10.0),
                Arrays.asList(0.0, 10.0),
                Arrays.asList(0.0, 0.0))));
    geometry.field("geometries", Arrays.asList(point, polygon));
    test1.field("geometry", geometry);
    test1.save();

    OResultSet execute =
        db.command(
            "SELECT from test where ST_Contains(geometry, ST_GeomFromText('POINT(1 1)')) = true");

    Assert.assertEquals(execute.stream().count(), 1);
  }

  @Test
  public void testGeoCollectionInsideTransaction() {
    db.begin();

    ODocument test1 = new ODocument("test");
    test1.field("name", "test1");
    ODocument geometry = new ODocument("OGeometryCollection");
    ODocument point = new ODocument("OPoint");
    point.field("coordinates", Arrays.asList(1.0, 2.0));
    ODocument polygon = new ODocument("OPolygon");
    polygon.field(
        "coordinates",
        Arrays.asList(
            Arrays.asList(
                Arrays.asList(0.0, 0.0),
                Arrays.asList(10.0, 0.0),
                Arrays.asList(10.0, 10.0),
                Arrays.asList(0.0, 10.0),
                Arrays.asList(0.0, 0.0))));
    geometry.field("geometries", Arrays.asList(point, polygon));
    test1.field("geometry", geometry);
    test1.save();

    db.commit();

    OResultSet execute =
        db.command(
            "SELECT from test where ST_Contains(geometry, ST_GeomFromText('POINT(1 1)')) = true");

    Assert.assertEquals(execute.stream().count(), 1);
  }
}
