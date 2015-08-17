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

import com.orientechnologies.lucene.shape.OMultiPolygonShapeBuilder;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.spatial4j.core.context.jts.JtsSpatialContext;
import com.spatial4j.core.shape.jts.JtsGeometry;
import com.vividsolutions.jts.geom.*;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Created by Enrico Risa on 07/08/15.
 */
public abstract class BaseSpatialLuceneTest extends BaseLuceneTest {

  protected JtsSpatialContext   context            = JtsSpatialContext.GEO;
  protected GeometryFactory     geometryFactory    = context.getGeometryFactory();

  protected static final String LINESTRINGWKT      = "LINESTRING (-71.160281 42.258729, -71.160837 42.259113, -71.161144 42.25932)";
  protected static final String POINTWKT           = "POINT (50 50)";

  protected static final String MULTILINESTRINGWKT = "MULTILINESTRING ((-71.160281 42.258729, -71.160837 42.259113, -71.161144 42.25932))";
  protected static final String MULTIPOINTWKT      = "MULTIPOINT ((-71.160281 42.258729), (-71.160837 42.259113), (-71.161144 42.25932))";

  protected static final String RECTANGLEWKT       = "POLYGON ((-45 -30, -45 30, 45 30, 45 -30, -45 -30))";

  protected static final String POLYGONWKT         = "POLYGON ((-71.1776585052917 42.3902909739571, -71.1776820268866 42.3903701743239,"
                                                       + " -71.1776063012595 42.3903825660754, -71.1775826583081 42.3903033653531, -71.1776585052917 42.3902909739571))";

  protected static final String MULTIPOLYGONWKT    = "MULTIPOLYGON (((15.520376205444336 38.23115539550781, 15.160243034362793 37.44404602050781, 15.309898376464844 37.134220123291016, 15.099987983703613 36.61998748779297, 14.33522891998291 36.99663162231445, 13.826732635498047 37.104530334472656, 12.43100357055664 37.61294937133789, 12.570943832397461 38.126380920410156, 13.741155624389648 38.03496551513672, 14.761248588562012 38.14387512207031, 15.520376205444336 38.23115539550781)), ((9.210012435913086 41.209991455078125, 9.809974670410156 40.50000762939453, 9.669519424438477 39.17737579345703, 9.214818000793457 39.240474700927734, 8.806936264038086 38.9066162109375, 8.428301811218262 39.17184829711914, 8.388253211975098 40.37831115722656, 8.159997940063477 40.950008392333984, 8.709991455078125 40.89998245239258, 9.210012435913086 41.209991455078125)), ((12.376484870910645 46.76755905151367, 13.806474685668945 46.509307861328125, 13.69810962677002 46.01677703857422, 13.937629699707031 45.59101486206055, 13.141606330871582 45.736690521240234, 12.328580856323242 45.381778717041016, 12.383874893188477 44.885372161865234, 12.261452674865723 44.60048294067383, 12.589237213134766 44.091365814208984, 13.52690601348877 43.58772659301758, 14.029821395874023 42.761009216308594, 15.142569541931152 41.95513916015625, 15.926191329956055 41.9613151550293, 16.169897079467773 41.74029541015625, 15.8893461227417 41.541080474853516, 16.785001754760742 41.17960739135742, 17.519168853759766 40.87714385986328, 18.376686096191406 40.35562515258789, 18.480247497558594 40.16886520385742, 18.293384552001953 39.81077575683594, 17.738380432128906 40.277671813964844, 16.869596481323242 40.44223403930664, 16.44874382019043 39.79540252685547, 17.171489715576172 39.42470169067383, 17.052841186523438 38.902870178222656, 16.635087966918945 38.843570709228516, 16.100961685180664 37.98590087890625, 15.684086799621582 37.90884780883789, 15.687962532043457 38.21459197998047, 15.89198112487793 38.75094223022461, 16.109331130981445 38.96454620361328, 15.7188138961792 39.544071197509766, 15.413613319396973 40.04835510253906, 14.998496055603027 40.172950744628906, 14.703268051147461 40.604549407958984, 14.06067180633545 40.786346435546875, 13.627985000610352 41.18828582763672, 12.888081550598145 41.253089904785156, 12.106682777404785 41.70453643798828, 11.191905975341797 42.35542678833008, 10.511947631835938 42.931461334228516, 10.200029373168945 43.920005798339844, 9.70248794555664 44.036277770996094, 8.888945579528809 44.366336822509766, 8.428561210632324 44.23122787475586, 7.850767135620117 43.767147064208984, 7.435184955596924 43.693843841552734, 7.549595832824707 44.127899169921875, 7.007562160491943 44.25476837158203, 6.749955177307129 45.02851867675781, 7.096652030944824 45.333099365234375, 6.80235481262207 45.708580017089844, 6.843593120574951 45.991146087646484, 7.273850917816162 45.776947021484375, 7.7559919357299805 45.82448959350586, 8.316630363464355 46.16364288330078, 8.489952087402344 46.005149841308594, 8.96630573272705 46.03693389892578, 9.182882308959961 46.440216064453125, 9.922837257385254 46.31489944458008, 10.363377571105957 46.48357009887695, 10.44270133972168 46.89354705810547, 11.048556327819824 46.75135803222656, 11.164828300476074 46.94157791137695, 12.153087615966797 47.115394592285156, 12.376484870910645 46.76755905151367)))";

  protected static final String GEOMETRYCOLLECTION = "GEOMETRYCOLLECTION (POINT (4 6), LINESTRING (4 6, 7 10))";

  @BeforeClass
  public void init() {
    initDB();
  }

  @AfterClass
  public void deInit() {
    deInitDB();
  }

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
    LinearRing linearRing = JtsSpatialContext.GEO.getGeometryFactory().createLinearRing(
        outerRing.toArray(new Coordinate[outerRing.size()]));
    LinearRing holeRing = JtsSpatialContext.GEO.getGeometryFactory().createLinearRing(hole.toArray(new Coordinate[hole.size()]));
    return JtsSpatialContext.GEO.getGeometryFactory().createPolygon(linearRing, new LinearRing[] { holeRing });
  }

  protected List<List<List<Double>>> polygonCoordTestHole() {
    return new ArrayList<List<List<Double>>>() {
      {
        add(new ArrayList<List<Double>>() {
          {
            add(Arrays.asList(100d, 1d));
            add(Arrays.asList(101d, 1d));
            add(Arrays.asList(101d, 0d));
            add(Arrays.asList(100d, 0d));
            add(Arrays.asList(100d, 1d));
          }
        });
        add(new ArrayList<List<Double>>() {
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
      ODocument location = new ODocument(type);
      location.field("coordinates", geometry.get("coordinates"));
      return location;
    } catch (Exception e) {
      e.printStackTrace();
    }
    return null;
  }

  protected GeometryCollection createGeometryCollection() {

    Point point = geometryFactory.createPoint(new Coordinate(4, 6));

    LineString lineString = geometryFactory.createLineString(new Coordinate[] { new Coordinate(4, 6), new Coordinate(7, 10) });

    return geometryFactory.createGeometryCollection(new Geometry[] { point, lineString });
  }

  protected ODocument geometryCollection() {

    final ODocument point = new ODocument("Point");
    point.field("coordinates", new ArrayList<Double>() {
      {
        add(4d);
        add(6d);
      }
    });

    final ODocument lineString = new ODocument("LineString");
    lineString.field("coordinates", new ArrayList<List<Double>>() {
      {
        add(Arrays.asList(4d, 6d));
        add(Arrays.asList(7d, 10d));
      }
    });

    ODocument geometryCollection = new ODocument("GeometryCollection");

    geometryCollection.field("geometries", new ArrayList<ODocument>() {
      {
        add(point);
        add(lineString);
      }
    });
    return geometryCollection;
  }

  protected ODocument lineStringDoc() {
    ODocument point = new ODocument("LineString");
    point.field("coordinates", new ArrayList<List<Double>>() {
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

    JtsGeometry geometry = builder.fromDoc(document);

    return (MultiPolygon) geometry.getGeom();

  }

  protected ODocument point() {
    ODocument point = new ODocument("Point");
    point.field("coordinates", new ArrayList<Double>() {
      {
        add(50d);
        add(50d);
      }
    });
    return point;
  }

  protected ODocument multiLineString() {
    ODocument point = new ODocument("MultiLineString");
    point.field("coordinates", new ArrayList<List<List<Double>>>() {
      {
        add(new ArrayList<List<Double>>() {
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
    ODocument point = new ODocument("MultiPoint");
    point.field("coordinates", new ArrayList<List<Double>>() {
      {
        add(Arrays.asList(-71.160281, 42.258729));
        add(Arrays.asList(-71.160837, 42.259113));
        add(Arrays.asList(-71.161144, 42.25932));
      }
    });
    return point;
  }

  protected ODocument rectangle() {
    ODocument polygon = new ODocument("Rectangle");
    polygon.field("coordinates", new ArrayList<Double>() {
      {
        add(-45d);
        add(-30d);
        add(45d);
        add(30d);
      }
    });
    return polygon;
  }

  protected ODocument polygon() {
    ODocument polygon = new ODocument("Polygon");
    polygon.field("coordinates", new ArrayList<List<List<Double>>>() {
      {
        add(new ArrayList<List<Double>>() {
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
