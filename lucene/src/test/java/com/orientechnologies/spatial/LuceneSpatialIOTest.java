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
import com.orientechnologies.spatial.shape.OGeometryCollectionShapeBuilder;
import com.orientechnologies.spatial.shape.OLineStringShapeBuilder;
import com.orientechnologies.spatial.shape.OMultiLineStringShapeBuilder;
import com.orientechnologies.spatial.shape.OMultiPointShapeBuilder;
import com.orientechnologies.spatial.shape.OMultiPolygonShapeBuilder;
import com.orientechnologies.spatial.shape.OPointShapeBuilder;
import com.orientechnologies.spatial.shape.OPolygonShapeBuilder;
import com.orientechnologies.spatial.shape.ORectangleShapeBuilder;
import com.orientechnologies.spatial.shape.OShapeFactory;
import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.GeometryCollection;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.MultiLineString;
import org.locationtech.jts.geom.MultiPoint;
import org.locationtech.jts.geom.MultiPolygon;
import org.locationtech.jts.geom.Polygon;
import org.locationtech.spatial4j.shape.Point;
import org.locationtech.spatial4j.shape.Rectangle;
import org.locationtech.spatial4j.shape.Shape;
import org.locationtech.spatial4j.shape.jts.JtsGeometry;

/** Created by Enrico Risa on 06/08/15. */
public class LuceneSpatialIOTest extends BaseSpatialLuceneTest {

  // POINT
  @Test
  public void testPointIO() throws ParseException, org.locationtech.jts.io.ParseException {

    ODocument doc = new ODocument("OPoint");
    doc.field(
        "coordinates",
        new ArrayList<Double>() {
          {
            add(-100d);
            add(45d);
          }
        });
    OPointShapeBuilder builder = new OPointShapeBuilder();

    String p1 = builder.asText(doc);
    Assert.assertNotNull(p1);

    Point point = context.makePoint(-100d, 45d);

    String p2 = context.getGeometryFrom(point).toText();

    Assert.assertEquals(p2, p1);

    ODocument parsed = builder.toDoc(p2);

    Assert.assertEquals(doc.<OPointShapeBuilder>field("coordinates"), parsed.field("coordinates"));
  }

  // MULTIPOINT
  @Test
  public void testMultiPointIO() {

    ODocument doc = new ODocument("OMultiPoint");
    doc.field(
        "coordinates",
        new ArrayList<List<Double>>() {
          {
            add(Arrays.asList(-71.160281, 42.258729));
            add(Arrays.asList(-71.160837, 42.259113));
            add(Arrays.asList(-71.161144, 42.25932));
          }
        });

    OMultiPointShapeBuilder builder = new OMultiPointShapeBuilder();
    String multiPoint = builder.asText(doc);

    List<Coordinate> points =
        new ArrayList<Coordinate>() {
          {
            add(new Coordinate(-71.160281, 42.258729));
            add(new Coordinate(-71.160837, 42.259113));
            add(new Coordinate(-71.161144, 42.25932));
          }
        };

    MultiPoint multiPoint1 =
        geometryFactory.createMultiPoint(points.toArray(new Coordinate[points.size()]));

    Assert.assertEquals(multiPoint1.toText(), multiPoint);
  }

  // RECTANGLE
  @Test
  @Ignore
  public void testRectangleIO() {

    ODocument doc = rectangle();

    ORectangleShapeBuilder builder = new ORectangleShapeBuilder();

    String rect = builder.asText(doc);

    Assert.assertNotNull(rect);

    Rectangle rectangle = context.makeRectangle(-45d, 45d, -30d, 30d);

    String rect1 = context.getGeometryFrom(rectangle).toText();

    Assert.assertEquals(rect1, rect);
  }

  // LINESTRING
  @Test
  public void testLineStringIO() {

    ODocument doc = new ODocument("OLineString");
    doc.field(
        "coordinates",
        new ArrayList<List<Double>>() {
          {
            add(Arrays.asList(-71.160281, 42.258729));
            add(Arrays.asList(-71.160837, 42.259113));
            add(Arrays.asList(-71.161144, 42.25932));
          }
        });

    OLineStringShapeBuilder builder = new OLineStringShapeBuilder();
    String lineString = builder.asText(doc);

    Shape shape =
        context.makeLineString(
            new ArrayList<Point>() {
              {
                add(context.makePoint(-71.160281, 42.258729));
                add(context.makePoint(-71.160837, 42.259113));
                add(context.makePoint(-71.161144, 42.25932));
              }
            });

    String lineString1 = context.getGeometryFrom(shape).toText();

    Assert.assertEquals(lineString1, lineString);
  }

  // MULTILINESTRING
  @Test
  public void testMultiLineStringIO() {

    ODocument doc = new ODocument("OMultiLineString");
    doc.field(
        "coordinates",
        new ArrayList<List<List<Double>>>() {
          {
            add(
                new ArrayList<List<Double>>() {
                  {
                    add(Arrays.asList(-71.160281, 42.258729));
                    add(Arrays.asList(-71.160837, 42.259113));
                    add(Arrays.asList(-71.161144, 42.25932));
                  }
                });
          }
        });

    OMultiLineStringShapeBuilder builder = new OMultiLineStringShapeBuilder();
    String multiLineString = builder.asText(doc);

    Shape shape =
        context.makeLineString(
            new ArrayList<Point>() {
              {
                add(context.makePoint(-71.160281, 42.258729));
                add(context.makePoint(-71.160837, 42.259113));
                add(context.makePoint(-71.161144, 42.25932));
              }
            });

    JtsGeometry geometry = (JtsGeometry) shape;

    LineString lineString = (LineString) geometry.getGeom();
    MultiLineString multiLineString2 =
        geometryFactory.createMultiLineString(new LineString[] {lineString});
    String multiLineString1 = multiLineString2.toText();

    Assert.assertEquals(multiLineString1, multiLineString);
  }

  // POLYGON
  @Test
  public void testPolygonNoHolesIO() {

    ODocument doc = new ODocument("OPolygon");
    doc.field(
        "coordinates",
        new ArrayList<List<List<Double>>>() {
          {
            add(
                new ArrayList<List<Double>>() {
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

    List<Coordinate> coordinates = new ArrayList<Coordinate>();
    coordinates.add(new Coordinate(-45, 30));
    coordinates.add(new Coordinate(45, 30));
    coordinates.add(new Coordinate(45, -30));
    coordinates.add(new Coordinate(-45, -30));
    coordinates.add(new Coordinate(-45, 30));

    OPolygonShapeBuilder builder = new OPolygonShapeBuilder();

    String p1 = builder.asText(doc);
    Polygon polygon1 =
        geometryFactory.createPolygon(coordinates.toArray(new Coordinate[coordinates.size()]));
    String p2 = polygon1.toText();
    Assert.assertEquals(p2, p1);
  }

  // POLYGON
  @Test
  public void testPolygonHolesIO() {

    ODocument doc = new ODocument("OPolygon");
    doc.field("coordinates", polygonCoordTestHole());

    Polygon polygon1 = polygonTestHole();

    OPolygonShapeBuilder builder = new OPolygonShapeBuilder();
    String p1 = builder.asText(doc);

    String p2 = polygon1.toText();
    Assert.assertEquals(p2, p1);
  }

  // MULTIPOLYGON
  @Test
  public void testMultiPolygon() throws IOException {

    OMultiPolygonShapeBuilder builder = new OMultiPolygonShapeBuilder();
    ODocument multiPolygon = loadMultiPolygon();
    MultiPolygon multiPolygon1 = createMultiPolygon();

    String m1 = builder.asText(multiPolygon);
    String m2 = multiPolygon1.toText();
    Assert.assertEquals(m2, m1);
  }

  // GEOMETRY COLLECTION
  @Test
  public void testGeometryCollection() throws IOException {

    OGeometryCollectionShapeBuilder builder =
        new OGeometryCollectionShapeBuilder(OShapeFactory.INSTANCE);
    ODocument geometryCollection = geometryCollection();
    GeometryCollection collection = createGeometryCollection();

    String m1 = builder.asText(geometryCollection);
    String m2 = collection.toText();
    Assert.assertEquals(m2, m1);
  }
}
