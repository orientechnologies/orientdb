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

  protected JtsSpatialContext context         = JtsSpatialContext.GEO;
  protected GeometryFactory   geometryFactory = context.getGeometryFactory();

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

  protected ODocument loadMultiPolygon() throws IOException {
    InputStream systemResourceAsStream = ClassLoader.getSystemResourceAsStream("italy.json");

    ODocument doc = new ODocument().fromJSON(systemResourceAsStream);

    Map geometry = doc.field("geometry");

    String type = (String) geometry.get("type");
    ODocument location = new ODocument(type);
    location.field("coordinates", geometry.get("coordinates"));
    return location;
  }

  protected MultiPolygon createMultiPolygon() throws IOException {

    ODocument document = loadMultiPolygon();

    OMultiPolygonShapeBuilder builder = new OMultiPolygonShapeBuilder();

    JtsGeometry geometry = builder.fromDoc(document);

    return (MultiPolygon) geometry.getGeom();

  }
}
