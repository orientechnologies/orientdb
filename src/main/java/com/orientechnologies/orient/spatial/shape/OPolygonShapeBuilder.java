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

package com.orientechnologies.orient.spatial.shape;

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OSchemaProxy;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.spatial4j.core.shape.jts.JtsGeometry;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.LineString;
import com.vividsolutions.jts.geom.LinearRing;
import com.vividsolutions.jts.geom.Polygon;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by enricorisa on 24/04/14.
 */
public class OPolygonShapeBuilder extends OComplexShapeBuilder<JtsGeometry> {
  @Override
  public String getName() {
    return "Polygon";
  }

  @Override
  public OShapeType getType() {
    return OShapeType.POLYGON;
  }

  @Override
  public void initClazz(ODatabaseDocumentTx db) {

    OSchemaProxy schema = db.getMetadata().getSchema();
    OClass polygon = schema.createClass(getName());
    polygon.addSuperClass(superClass(db));
    polygon.createProperty("coordinates", OType.EMBEDDEDLIST, OType.EMBEDDEDLIST);

  }

  @Override
  public JtsGeometry fromDoc(ODocument document) {
    validate(document);
    List<List<List<Number>>> coordinates = document.field("coordinates");

    return toShape(createPolygon(coordinates));
  }

  protected Polygon createPolygon(List<List<List<Number>>> coordinates) {
    Polygon shape;
    if (coordinates.size() == 1) {
      List<List<Number>> coords = coordinates.get(0);
      LinearRing linearRing = createLinearRing(coords);
      shape = GEOMETRY_FACTORY.createPolygon(linearRing);
    } else {
      int i = 0;
      LinearRing outerRing = null;
      LinearRing[] holes = new LinearRing[coordinates.size() - 1];
      for (List<List<Number>> coordinate : coordinates) {
        if (i == 0) {
          outerRing = createLinearRing(coordinate);
        } else {
          holes[i - 1] = createLinearRing(coordinate);
        }
        i++;
      }
      shape = GEOMETRY_FACTORY.createPolygon(outerRing, holes);
    }
    return shape;
  }

  protected LinearRing createLinearRing(List<List<Number>> coords) {
    Coordinate[] crs = new Coordinate[coords.size()];
    int i = 0;
    for (List<Number> points : coords) {
      crs[i] = new Coordinate(points.get(0).doubleValue(), points.get(1).doubleValue());
      i++;
    }
    return GEOMETRY_FACTORY.createLinearRing(crs);
  }

  @Override
  public JtsGeometry fromText(String wkt) {
    return null;
  }

  @Override
  public ODocument toDoc(JtsGeometry shape) {

    ODocument doc = new ODocument(getName());
    Polygon polygon = (Polygon) shape.getGeom();
    List<List<List<Double>>> polyCoordinates = coordinatesFromPolygon(polygon);
    doc.field(COORDINATES, polyCoordinates);
    return doc;
  }

  protected List<List<List<Double>>> coordinatesFromPolygon(Polygon polygon) {
    List<List<List<Double>>> polyCoordinates = new ArrayList<List<List<Double>>>();
    LineString exteriorRing = polygon.getExteriorRing();
    polyCoordinates.add(coordinatesFromLineString(exteriorRing));
    int i = polygon.getNumInteriorRing();
    for (int j = 0; j < i; j++) {
      LineString interiorRingN = polygon.getInteriorRingN(j);
      polyCoordinates.add(coordinatesFromLineString(interiorRingN));
    }
    return polyCoordinates;
  }
}
