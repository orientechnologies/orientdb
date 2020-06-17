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
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.spatial.shape.legacy.OPointLegecyBuilder;
import java.io.IOException;
import java.util.List;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

/** Created by Enrico Risa on 13/08/15. */
public class LuceneSpatialFunctionFromTextTest extends BaseSpatialLuceneTest {

  @Test
  public void geomFromTextLineStringTest() {

    ODocument point = lineStringDoc();
    checkFromText(point, "select ST_GeomFromText('" + LINESTRINGWKT + "') as geom");
  }

  protected void checkFromText(ODocument source, String query) {

    List<ODocument> docs = db.command(new OCommandSQL(query)).execute();

    Assert.assertEquals(docs.size(), 1);
    ODocument geom = docs.get(0).field("geom");
    assertGeometry(source, geom);
  }

  private void assertGeometry(ODocument source, ODocument geom) {
    Assert.assertNotNull(geom);

    Assert.assertNotNull(geom.field("coordinates"));

    Assert.assertEquals(source.getClassName(), geom.getClassName());
    Assert.assertEquals(
        geom.<OPointLegecyBuilder>field("coordinates"), source.field("coordinates"));
  }

  @Test
  public void geomFromTextMultiLineStringTest() {

    ODocument point = multiLineString();
    checkFromText(point, "select ST_GeomFromText('" + MULTILINESTRINGWKT + "') as geom");
  }

  @Test
  public void geomFromTextPointTest() {

    ODocument point = point();
    checkFromText(point, "select ST_GeomFromText('" + POINTWKT + "') as geom");
  }

  @Test
  public void geomFromTextMultiPointTest() {

    ODocument point = multiPoint();
    checkFromText(point, "select ST_GeomFromText('" + MULTIPOINTWKT + "') as geom");
  }

  // TODO enable
  @Test
  @Ignore
  public void geomFromTextRectangleTest() {
    ODocument polygon = rectangle();
    // RECTANGLE
    checkFromText(polygon, "select ST_GeomFromText('" + RECTANGLEWKT + "') as geom");
  }

  @Test
  public void geomFromTextPolygonTest() {
    ODocument polygon = polygon();
    checkFromText(polygon, "select ST_GeomFromText('" + POLYGONWKT + "') as geom");
  }

  @Test
  public void geomFromTextMultiPolygonTest() throws IOException {
    ODocument polygon = loadMultiPolygon();

    checkFromText(polygon, "select ST_GeomFromText('" + MULTIPOLYGONWKT + "') as geom");
  }

  @Test
  public void geomCollectionFromText() {
    checkFromCollectionText(
        geometryCollection(), "select ST_GeomFromText('" + GEOMETRYCOLLECTION + "') as geom");
  }

  protected void checkFromCollectionText(ODocument source, String query) {

    List<ODocument> docs = db.command(new OCommandSQL(query)).execute();

    Assert.assertEquals(docs.size(), 1);
    ODocument geom = docs.get(0).field("geom");
    Assert.assertNotNull(geom);

    Assert.assertNotNull(geom.field("geometries"));

    Assert.assertEquals(source.getClassName(), geom.getClassName());

    List<ODocument> sourceCollection = source.field("geometries");
    List<ODocument> targetCollection = source.field("geometries");
    Assert.assertEquals(sourceCollection.size(), targetCollection.size());

    int i = 0;
    for (ODocument entries : sourceCollection) {
      assertGeometry(entries, targetCollection.get(i));
      i++;
    }
  }
}
