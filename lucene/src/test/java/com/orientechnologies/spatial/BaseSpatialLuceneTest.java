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

import com.orientechnologies.lucene.test.BaseLuceneTest;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.spatial.shape.OMultiPolygonShapeBuilder;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryCollection;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.LinearRing;
import org.locationtech.jts.geom.MultiPolygon;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.Polygon;
import org.locationtech.spatial4j.context.jts.JtsSpatialContext;
import org.locationtech.spatial4j.shape.Shape;
import org.locationtech.spatial4j.shape.jts.JtsGeometry;

/** Created by Enrico Risa on 07/08/15. */
public abstract class BaseSpatialLuceneTest extends BaseLuceneTest {

  protected static final String LINESTRINGWKT =
      "LINESTRING (-71.160281 42.258729, -71.160837 42.259113, -71.161144 42.25932)";
  protected static final String POINTWKT = "POINT (50 50)";
  protected static final String MULTILINESTRINGWKT =
      "MULTILINESTRING ((-71.160281 42.258729, -71.160837 42.259113, -71.161144 42.25932))";
  protected static final String MULTIPOINTWKT =
      "MULTIPOINT ((-71.160281 42.258729), (-71.160837 42.259113), (-71.161144 42.25932))";
  protected static final String RECTANGLEWKT =
      "POLYGON ((-45 -30, -45 30, 45 30, 45 -30, -45 -30))";
  protected static final String POLYGONWKT =
      "POLYGON ((-71.1776585052917 42.3902909739571, -71.1776820268866 42.3903701743239,"
          + " -71.1776063012595 42.3903825660754, -71.1775826583081 42.3903033653531, -71.1776585052917 42.3902909739571))";
  protected static final String MULTIPOLYGONWKT =
      "MULTIPOLYGON (((15.520376 38.231155, 15.160243 37.444046, 15.309898 37.134219, 15.099988 36.619987, 14.335229 36.996631, 13.826733 37.104531, 12.431004 37.61295, 12.570944 38.126381, 13.741156 38.034966, 14.761249 38.143874, 15.520376 38.231155)), ((9.210012 41.209991, 9.809975 40.500009, 9.669519 39.177376, 9.214818 39.240473, 8.806936 38.906618, 8.428302 39.171847, 8.388253 40.378311, 8.159998 40.950007, 8.709991 40.899984, 9.210012 41.209991)), ((12.376485 46.767559, 13.806475 46.509306, 13.69811 46.016778, 13.93763 45.591016, 13.141606 45.736692, 12.328581 45.381778, 12.383875 44.885374, 12.261453 44.600482, 12.589237 44.091366, 13.526906 43.587727, 14.029821 42.761008, 15.14257 41.95514, 15.926191 41.961315, 16.169897 41.740295, 15.889346 41.541082, 16.785002 41.179606, 17.519169 40.877143, 18.376687 40.355625, 18.480247 40.168866, 18.293385 39.810774, 17.73838 40.277671, 16.869596 40.442235, 16.448743 39.795401, 17.17149 39.4247, 17.052841 38.902871, 16.635088 38.843572, 16.100961 37.985899, 15.684087 37.908849, 15.687963 38.214593, 15.891981 38.750942, 16.109332 38.964547, 15.718814 39.544072, 15.413613 40.048357, 14.998496 40.172949, 14.703268 40.60455, 14.060672 40.786348, 13.627985 41.188287, 12.888082 41.25309, 12.106683 41.704535, 11.191906 42.355425, 10.511948 42.931463, 10.200029 43.920007, 9.702488 44.036279, 8.888946 44.366336, 8.428561 44.231228, 7.850767 43.767148, 7.435185 43.693845, 7.549596 44.127901, 7.007562 44.254767, 6.749955 45.028518, 7.096652 45.333099, 6.802355 45.70858, 6.843593 45.991147, 7.273851 45.776948, 7.755992 45.82449, 8.31663 46.163642, 8.489952 46.005151, 8.966306 46.036932, 9.182882 46.440215, 9.922837 46.314899, 10.363378 46.483571, 10.442701 46.893546, 11.048556 46.751359, 11.164828 46.941579, 12.153088 47.115393, 12.376485 46.767559)))";
  protected static final String GEOMETRYCOLLECTION =
      "GEOMETRYCOLLECTION (POINT (4 6), LINESTRING (4 6, 7 10))";
  protected JtsSpatialContext context = JtsSpatialContext.GEO;
  protected GeometryFactory geometryFactory = context.getGeometryFactory();

  protected Polygon polygonTestHole() {
    List<Coordinate> outerRing = new ArrayList<Coordinate>();
    outerRing.add(new Coordinate(100.0, 1.0));
    outerRing.add(new Coordinate(101.0, 1.0));
    outerRing.add(new Coordinate(101.0, 0.0));
    outerRing.add(new Coordinate(100.0, 0.0));
    outerRing.add(new Coordinate(100.0, 1.0));

    List<Coordinate> hole = new ArrayList<Coordinate>();
    hole.add(new Coordinate(100.2, 0.8));
    hole.add(new Coordinate(100.2, 0.2));
    hole.add(new Coordinate(100.8, 0.2));
    hole.add(new Coordinate(100.8, 0.8));
    hole.add(new Coordinate(100.2, 0.8));
    LinearRing linearRing =
        JtsSpatialContext.GEO
            .getGeometryFactory()
            .createLinearRing(outerRing.toArray(new Coordinate[outerRing.size()]));
    LinearRing holeRing =
        JtsSpatialContext.GEO
            .getGeometryFactory()
            .createLinearRing(hole.toArray(new Coordinate[hole.size()]));
    return JtsSpatialContext.GEO
        .getGeometryFactory()
        .createPolygon(linearRing, new LinearRing[] {holeRing});
  }

  protected List<List<List<Double>>> polygonCoordTestHole() {
    return new ArrayList<List<List<Double>>>() {
      {
        add(
            new ArrayList<List<Double>>() {
              {
                add(Arrays.asList(100d, 1d));
                add(Arrays.asList(101d, 1d));
                add(Arrays.asList(101d, 0d));
                add(Arrays.asList(100d, 0d));
                add(Arrays.asList(100d, 1d));
              }
            });
        add(
            new ArrayList<List<Double>>() {
              {
                add(Arrays.asList(100.2d, 0.8d));
                add(Arrays.asList(100.2d, 0.2d));
                add(Arrays.asList(100.8d, 0.2d));
                add(Arrays.asList(100.8d, 0.8d));
                add(Arrays.asList(100.2d, 0.8d));
              }
            });
      }
    };
  }

  protected ODocument loadMultiPolygon() {

    try {
      InputStream systemResourceAsStream = ClassLoader.getSystemResourceAsStream("italy.json");

      ODocument doc = new ODocument().fromJSON(systemResourceAsStream);

      Map geometry = doc.field("geometry");

      String type = (String) geometry.get("type");
      ODocument location = new ODocument("O" + type);
      location.field("coordinates", geometry.get("coordinates"));
      return location;
    } catch (Exception e) {
      e.printStackTrace();
    }
    return null;
  }

  protected GeometryCollection createGeometryCollection() {

    Point point = geometryFactory.createPoint(new Coordinate(4, 6));

    LineString lineString =
        geometryFactory.createLineString(
            new Coordinate[] {new Coordinate(4, 6), new Coordinate(7, 10)});

    return geometryFactory.createGeometryCollection(new Geometry[] {point, lineString});
  }

  protected ODocument geometryCollection() {

    final ODocument point = new ODocument("OPoint");
    point.field(
        "coordinates",
        new ArrayList<Double>() {
          {
            add(4d);
            add(6d);
          }
        });

    final ODocument lineString = new ODocument("OLineString");
    lineString.field(
        "coordinates",
        new ArrayList<List<Double>>() {
          {
            add(Arrays.asList(4d, 6d));
            add(Arrays.asList(7d, 10d));
          }
        });

    ODocument geometryCollection = new ODocument("OGeometryCollection");

    geometryCollection.field(
        "geometries",
        new ArrayList<ODocument>() {
          {
            add(point);
            add(lineString);
          }
        });
    return geometryCollection;
  }

  protected ODocument lineStringDoc() {
    ODocument point = new ODocument("OLineString");
    point.field(
        "coordinates",
        new ArrayList<List<Double>>() {
          {
            add(Arrays.asList(-71.160281, 42.258729));
            add(Arrays.asList(-71.160837, 42.259113));
            add(Arrays.asList(-71.161144, 42.25932));
          }
        });
    return point;
  }

  protected MultiPolygon createMultiPolygon() throws IOException {

    ODocument document = loadMultiPolygon();

    OMultiPolygonShapeBuilder builder = new OMultiPolygonShapeBuilder();

    Shape geometry = builder.fromDoc(document);

    return (MultiPolygon) ((JtsGeometry) geometry).getGeom();
  }

  protected ODocument point() {
    ODocument point = new ODocument("OPoint");
    point.field(
        "coordinates",
        new ArrayList<Double>() {
          {
            add(50d);
            add(50d);
          }
        });
    return point;
  }

  protected ODocument multiLineString() {
    ODocument point = new ODocument("OMultiLineString");
    point.field(
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
    return point;
  }

  protected ODocument multiPoint() {
    ODocument point = new ODocument("OMultiPoint");
    point.field(
        "coordinates",
        new ArrayList<List<Double>>() {
          {
            add(Arrays.asList(-71.160281, 42.258729));
            add(Arrays.asList(-71.160837, 42.259113));
            add(Arrays.asList(-71.161144, 42.25932));
          }
        });
    return point;
  }

  protected ODocument rectangle() {
    ODocument polygon = new ODocument("OPolygon");
    polygon.field(
        "coordinates",
        new ArrayList<List<List<Double>>>() {
          {
            add(
                new ArrayList<List<Double>>() {
                  {
                    add(Arrays.asList(-45d, -30d));
                    add(Arrays.asList(-45d, 30d));
                    add(Arrays.asList(45d, 30d));
                    add(Arrays.asList(45d, 30d));
                    add(Arrays.asList(-45d, -30d));
                  }
                });
          }
        });
    return polygon;
  }

  protected ODocument polygon() {
    ODocument polygon = new ODocument("OPolygon");
    polygon.field(
        "coordinates",
        new ArrayList<List<List<Double>>>() {
          {
            add(
                new ArrayList<List<Double>>() {
                  {
                    add(Arrays.asList(-71.1776585052917, 42.3902909739571));
                    add(Arrays.asList(-71.1776820268866, 42.3903701743239));
                    add(Arrays.asList(-71.1776063012595, 42.3903825660754));
                    add(Arrays.asList(-71.1775826583081, 42.3903033653531));
                    add(Arrays.asList(-71.1776585052917, 42.3902909739571));
                  }
                });
          }
        });
    return polygon;
  }
}
