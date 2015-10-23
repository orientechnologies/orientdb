/*
 *
 *  * Copyright 2014 Orient Technologies.
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  *      http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *  
 */

package com.orientechnologies.lucene.test.geo;

import com.orientechnologies.lucene.test.BaseSpatialLuceneTest;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.List;

/**
 * Created by Enrico Risa on 13/08/15.
 */
@Test(groups = "embedded")
public class LuceneSpatialFunctionFromTextTest extends BaseSpatialLuceneTest {


  @BeforeClass
  @Override
  public void init() {
    super.init();
  }

  @AfterClass
  @Override
  public void deInit() {
    super.deInit();
  }

  @Override
  protected String getDatabaseName() {
    return "spatialFunctionAsText";
  }

  @Test
  public void geomFromTextLineStringTest() {

    ODocument point = lineStringDoc();
    checkFromText(point, "select ST_GeomFromText('" + LINESTRINGWKT + "') as geom");
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

  //TODO enable
  @Test(enabled = false)
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
    checkFromCollectionText(geometryCollection(), "select ST_GeomFromText('" + GEOMETRYCOLLECTION + "') as geom");
  }

  protected void checkFromCollectionText(ODocument source, String query) {

    List<ODocument> docs = databaseDocumentTx.command(new OCommandSQL(query)).execute();

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

  protected void checkFromText(ODocument source, String query) {

    List<ODocument> docs = databaseDocumentTx.command(new OCommandSQL(query)).execute();

    Assert.assertEquals(docs.size(), 1);
    ODocument geom = docs.get(0).field("geom");
    assertGeometry(source, geom);

  }

  private void assertGeometry(ODocument source, ODocument geom) {
    Assert.assertNotNull(geom);

    Assert.assertNotNull(geom.field("coordinates"));

    Assert.assertEquals(source.getClassName(), geom.getClassName());
    Assert.assertEquals(geom.field("coordinates"), source.field("coordinates"));
  }

}
