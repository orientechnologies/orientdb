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
import com.vividsolutions.jts.geom.MultiPoint;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

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
  public void initClazz(ODatabaseDocumentTx db) {

    OSchemaProxy schema = db.getMetadata().getSchema();
    OClass multiPoint = schema.createAbstractClass(getName(),superClass(db));
    multiPoint.createProperty(COORDINATES, OType.EMBEDDEDLIST, OType.EMBEDDEDLIST);
  }

  @Override
  public ODocument toDoc(final JtsGeometry shape) {
    final MultiPoint geom = (MultiPoint) shape.getGeom();

    ODocument doc = new ODocument(getName());
    doc.field(COORDINATES, new ArrayList<List<Double>>() {
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
