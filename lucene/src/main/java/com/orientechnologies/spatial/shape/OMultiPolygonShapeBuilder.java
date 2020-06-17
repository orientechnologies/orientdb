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

import com.orientechnologies.orient.core.db.ODatabaseInternal;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import java.util.ArrayList;
import java.util.List;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.MultiPolygon;
import org.locationtech.jts.geom.Polygon;
import org.locationtech.spatial4j.shape.jts.JtsGeometry;

/** Created by enricorisa on 24/04/14. */
public class OMultiPolygonShapeBuilder extends OPolygonShapeBuilder {
  @Override
  public String getName() {
    return "OMultiPolygon";
  }

  @Override
  public OShapeType getType() {
    return OShapeType.MULTIPOLYGON;
  }

  @Override
  public void initClazz(ODatabaseInternal db) {

    OSchema schema = db.getMetadata().getSchema();
    OClass polygon = schema.createAbstractClass(getName(), superClass(db));
    polygon.createProperty("coordinates", OType.EMBEDDEDLIST, OType.EMBEDDEDLIST);
  }

  @Override
  public JtsGeometry fromDoc(ODocument document) {
    validate(document);
    List<List<List<List<Number>>>> coordinates = document.field("coordinates");

    Polygon[] polygons = new Polygon[coordinates.size()];
    int i = 0;
    for (List<List<List<Number>>> coordinate : coordinates) {
      polygons[i] = createPolygon(coordinate);
      i++;
    }
    return toShape(GEOMETRY_FACTORY.createMultiPolygon(polygons));
  }

  @Override
  public ODocument toDoc(JtsGeometry shape) {

    ODocument doc = new ODocument(getName());
    MultiPolygon multiPolygon = (MultiPolygon) shape.getGeom();
    List<List<List<List<Double>>>> polyCoordinates = new ArrayList<List<List<List<Double>>>>();
    int n = multiPolygon.getNumGeometries();

    for (int i = 0; i < n; i++) {
      Geometry geom = multiPolygon.getGeometryN(i);
      polyCoordinates.add(coordinatesFromPolygon((Polygon) geom));
    }

    doc.field(COORDINATES, polyCoordinates);
    return doc;
  }
}
