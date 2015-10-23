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
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.spatial4j.core.shape.Shape;
import com.vividsolutions.jts.geom.Geometry;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.text.ParseException;
import java.util.List;

/**
 * Created by Enrico Risa on 14/08/15.
 */
@Test(groups = "embedded")
public class LuceneSpatialFunctionAsTextTest extends BaseSpatialLuceneTest {

  @Override
  protected String getDatabaseName() {
    return "functionAsText";
  }

  @BeforeClass
  @Override
  public void init() {
    super.init();

    OSchema schema = databaseDocumentTx.getMetadata().getSchema();
    OClass v = schema.getClass("V");
    OClass oClass = schema.createClass("Location");
    oClass.setSuperClass(v);
    oClass.createProperty("geometry", OType.EMBEDDED, schema.getClass("OShape"));
    oClass.createProperty("name", OType.STRING);

    initData();
  }

  private void initData() {

    createLocation("OPoint", point());
    createLocation("OMultiPoint", multiPoint());
    createLocation("OLineString", lineStringDoc());
    createLocation("OMultiLineString", multiLineString());
    createLocation("ORectangle", rectangle());
    createLocation("OPolygon", polygon());
    createLocation("OMultiPolygon", loadMultiPolygon());
    createLocation("OGeometryCollection", geometryCollection());

  }

  @Test
  public void testPoint() {

    queryAndAssertGeom("OPoint", POINTWKT);

  }

  @Test
  public void testMultiPoint() {

    queryAndAssertGeom("OMultiPoint", MULTIPOINTWKT);

  }

  @Test
  public void testLineString() {
    queryAndAssertGeom("OLineString", LINESTRINGWKT);
  }

  @Test
  public void testMultiLineString() {
    queryAndAssertGeom("OMultiLineString", MULTILINESTRINGWKT);
  }

  @Test(enabled = false)
  public void testRectangle() {
    queryAndAssertGeom("ORectangle", RECTANGLEWKT);
  }

  @Test
  public void testBugEnvelope() {
    try {
      Shape shape = context.readShapeFromWkt(RECTANGLEWKT);

      Geometry geometryFrom = context.getGeometryFrom(shape);
      Assert.assertEquals(geometryFrom.toText(), RECTANGLEWKT);
    } catch (ParseException e) {
      e.printStackTrace();
    }
  }

  @Test
  public void testPolygon() {
    queryAndAssertGeom("OPolygon", POLYGONWKT);
  }

  @Test
  public void testGeometryCollection() {
    queryAndAssertGeom("OGeometryCollection", GEOMETRYCOLLECTION);
  }

  @Test
  public void testMultiPolygon() {
    queryAndAssertGeom("OMultiPolygon", MULTIPOLYGONWKT);
  }

  protected void queryAndAssertGeom(String name, String wkt) {
    List<ODocument> results = databaseDocumentTx.command(
        new OCommandSQL("select *, ST_AsText(geometry) as text from Location where name = ? ")).execute(name);

    Assert.assertEquals(1, results.size());
    ODocument doc = results.iterator().next();

    String asText = doc.field("text");

    Assert.assertNotNull(asText);
    Assert.assertEquals(asText, wkt);
  }

  protected void createLocation(String name, ODocument geometry) {
    ODocument doc = new ODocument("Location");
    doc.field("name", name);
    doc.field("geometry", geometry);
    databaseDocumentTx.save(doc);
  }
}
