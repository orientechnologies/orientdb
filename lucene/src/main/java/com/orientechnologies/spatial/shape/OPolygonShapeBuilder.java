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
package com.orientechnologies.spatial.shape;

import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.ODatabaseInternal;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.locationtech.jts.geom.*;
import org.locationtech.spatial4j.shape.jts.JtsGeometry;

/** Created by enricorisa on 24/04/14. */
public class OPolygonShapeBuilder extends OComplexShapeBuilder<JtsGeometry> {
  @Override
  public String getName() {
    return "OPolygon";
  }

  @Override
  public OShapeType getType() {
    return OShapeType.POLYGON;
  }

  @Override
  public void initClazz(ODatabaseInternal db) {

    OSchema schema = db.getMetadata().getSchema();
    OClass polygon = schema.createAbstractClass(getName(), superClass(db));
    polygon.createProperty(COORDINATES, OType.EMBEDDEDLIST, OType.EMBEDDEDLIST);

    if (OGlobalConfiguration.SPATIAL_ENABLE_DIRECT_WKT_READER.getValueAsBoolean()) {
      OClass polygonZ = schema.createAbstractClass(getName() + "Z", superClass(db));
      polygonZ.createProperty(COORDINATES, OType.EMBEDDEDLIST, OType.EMBEDDEDLIST);
    }
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
  public ODocument toDoc(JtsGeometry shape) {

    ODocument doc = new ODocument(getName());
    Polygon polygon = (Polygon) shape.getGeom();
    List<List<List<Double>>> polyCoordinates = coordinatesFromPolygon(polygon);
    doc.field(COORDINATES, polyCoordinates);
    return doc;
  }

  @Override
  protected ODocument toDoc(JtsGeometry shape, Geometry geometry) {
    if (geometry == null || Double.isNaN(geometry.getCoordinate().getZ())) {
      return toDoc(shape);
    }

    ODocument doc = new ODocument(getName() + "Z");
    Polygon polygon = (Polygon) shape.getGeom();
    List<List<List<Double>>> polyCoordinates = coordinatesFromPolygonZ(geometry);
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

  protected List<List<List<Double>>> coordinatesFromPolygonZ(Geometry polygon) {
    List<List<List<Double>>> polyCoordinates = new ArrayList<>();
    for (int i = 0; i < polygon.getNumGeometries(); i++) {
      polyCoordinates.add(coordinatesFromLineStringZ(polygon.getGeometryN(i)));
    }
    return polyCoordinates;
  }

  @Override
  public String asText(ODocument document) {
    if (document.getClassName().equals("OPolygonZ")) {
      List<List<List<Double>>> coordinates = document.getProperty("coordinates");

      String result =
          coordinates.stream()
              .map(
                  poly ->
                      "("
                          + poly.stream()
                              .map(
                                  point ->
                                      (point.stream()
                                          .map(coord -> format(coord))
                                          .collect(Collectors.joining(" "))))
                              .collect(Collectors.joining(", "))
                          + ")")
              .collect(Collectors.joining(" "));
      return "POLYGON Z (" + result + ")";

    } else {
      return super.asText(document);
    }
  }
}
