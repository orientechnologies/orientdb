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

import com.spatial4j.core.context.jts.JtsSpatialContext;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.LinearRing;
import com.vividsolutions.jts.geom.Polygon;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Created by Enrico Risa on 07/08/15.
 */
public abstract class BaseSpatialLuceneTest extends BaseLuceneTest {

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
}
