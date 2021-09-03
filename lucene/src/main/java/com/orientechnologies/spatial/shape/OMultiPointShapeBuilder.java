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
import java.util.Arrays;
import java.util.List;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.MultiPoint;
import org.locationtech.spatial4j.shape.jts.JtsGeometry;

public class OMultiPointShapeBuilder extends OComplexShapeBuilder<JtsGeometry> {
  @Override
  public String getName() {
    return "OMultiPoint";
  }

  @Override
  public OShapeType getType() {
    return OShapeType.MULTIPOINT;
  }

  @Override
  public JtsGeometry fromDoc(ODocument document) {
    validate(document);
    List<List<Number>> coordinates = document.field(COORDINATES);
    Coordinate[] coords = new Coordinate[coordinates.size()];
    int i = 0;
    for (List<Number> coordinate : coordinates) {
      coords[i] = new Coordinate(coordinate.get(0).doubleValue(), coordinate.get(1).doubleValue());
      i++;
    }
    return toShape(GEOMETRY_FACTORY.createMultiPoint(coords));
  }

  @Override
  public void initClazz(ODatabaseInternal db) {

    OSchema schema = db.getMetadata().getSchema();
    OClass multiPoint = schema.createAbstractClass(getName(), superClass(db));
    multiPoint.createProperty(COORDINATES, OType.EMBEDDEDLIST, OType.EMBEDDEDLIST);
  }

  @Override
  public ODocument toDoc(final JtsGeometry shape) {
    final MultiPoint geom = (MultiPoint) shape.getGeom();

    ODocument doc = new ODocument(getName());
    doc.field(
        COORDINATES,
        new ArrayList<List<Double>>() {
          {
            Coordinate[] coordinates = geom.getCoordinates();
            for (Coordinate coordinate : coordinates) {
              add(Arrays.asList(coordinate.x, coordinate.y));
            }
          }
        });
    return doc;
  }
}
