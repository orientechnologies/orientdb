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

package com.orientechnologies.lucene.test;

import com.orientechnologies.lucene.shape.OPointShapeBuilder;
import com.orientechnologies.lucene.shape.OPolygonShapeBuilder;
import com.orientechnologies.lucene.shape.ORectangleShapeBuilder;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.spatial4j.core.context.SpatialContext;
import com.spatial4j.core.context.jts.JtsSpatialContext;
import com.spatial4j.core.shape.Point;
import com.spatial4j.core.shape.Rectangle;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Polygon;
import junit.framework.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Created by Enrico Risa on 06/08/15.
 */
public class LuceneSpatialIOTest extends BaseSpatialLuceneTest {
  @Override
  protected String getDatabaseName() {
    return "conversionTest";
  }

  @BeforeClass
  public void init() {
    initDB();
  }

  @Test
  public void testPointIO() {

    ODocument doc = new ODocument("Point");
    doc.field("coordinates", new ArrayList<Double>() {
      {
        add(-100d);
        add(45d);
      }
    });
    doc.save();
    OPointShapeBuilder builder = new OPointShapeBuilder();

    String p1 = builder.asText(doc);
    Assert.assertNotNull(p1);

    Point point = SpatialContext.GEO.makePoint(-100d, 45d);

    String p2 = JtsSpatialContext.GEO.getGeometryFrom(point).toText();

    Assert.assertEquals(p2, p1);
  }

  @Test
  public void testRectangleIO() {

    ODocument doc = new ODocument("Rectangle");
    doc.field("coordinates", new ArrayList<Double>() {
      {
        add(-45d);
        add(45d);
        add(-30d);
        add(30d);
      }
    });
    doc.save();

    ORectangleShapeBuilder builder = new ORectangleShapeBuilder();

    String rect = builder.asText(doc);

    Assert.assertNotNull(rect);

    Rectangle rectangle = JtsSpatialContext.GEO.makeRectangle(-45d, 45d, -30d, 30d);

    String rect1 = JtsSpatialContext.GEO.getGeometryFrom(rectangle).toText();

    Assert.assertEquals(rect1, rect);
  }

  public void testLineIO() {

  }

  public void testPolygonNoHolesIO() {

    ODocument doc = new ODocument("Polygon");
    doc.field("coordinates", new ArrayList<List<List<Double>>>() {
      {
        add(new ArrayList<List<Double>>() {
          {
            add(Arrays.asList(-45d, 30d));
            add(Arrays.asList(45d, 30d));
            add(Arrays.asList(45d, -30d));
            add(Arrays.asList(-45d, -30d));
            add(Arrays.asList(-45d, 30d));
          }
        });
      }
    });
    doc.save();

    List<Coordinate> coordinates = new ArrayList<Coordinate>();
    coordinates.add(new Coordinate(-45, 30));
    coordinates.add(new Coordinate(45, 30));
    coordinates.add(new Coordinate(45, -30));
    coordinates.add(new Coordinate(-45, -30));
    coordinates.add(new Coordinate(-45, 30));

    OPolygonShapeBuilder builder = new OPolygonShapeBuilder();

    String p1 = builder.asText(doc);
    Polygon polygon1 = JtsSpatialContext.GEO.getGeometryFactory().createPolygon(
        coordinates.toArray(new Coordinate[coordinates.size()]));
    String p2 = polygon1.toText();
    Assert.assertEquals(p2, p1);
  }

  public void testPolygonHolesIO() {

    ODocument doc = new ODocument("Polygon");
    doc.field("coordinates", polygonCoordTestHole());

    Polygon polygon1 = polygonTestHole();

    OPolygonShapeBuilder builder = new OPolygonShapeBuilder();
    String p1 = builder.asText(doc);

    String p2 = polygon1.toText();
    Assert.assertEquals(p2, p1);
  }

  @AfterClass
  public void deInit() {
    deInitDB();
  }
}
