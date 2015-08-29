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
import com.vividsolutions.jts.geom.LineString;
import com.vividsolutions.jts.geom.MultiLineString;

import java.util.ArrayList;
import java.util.List;

public class OMultiLineStringShapeBuilder extends OComplexShapeBuilder<JtsGeometry> {
  @Override
  public String getName() {
    return "MultiLineString";
  }

  @Override
  public OShapeType getType() {
    return OShapeType.MULTILINESTRING;
  }

  @Override
  public JtsGeometry fromDoc(ODocument document) {
    validate(document);
    List<List<List<Number>>> coordinates = document.field(COORDINATES);
    LineString[] multiLine = new LineString[coordinates.size()];
    int j = 0;
    for (List<List<Number>> coordinate : coordinates) {
      multiLine[j] = createLineString(coordinate);
      j++;
    }
    return toShape(GEOMETRY_FACTORY.createMultiLineString(multiLine));
  }

  @Override
  public void initClazz(ODatabaseDocumentTx db) {
    OSchemaProxy schema = db.getMetadata().getSchema();
    OClass lineString = schema.createClass(getName());
    lineString.addSuperClass(superClass(db));
    lineString.createProperty(COORDINATES, OType.EMBEDDEDLIST, OType.EMBEDDEDLIST);
  }

  @Override
  public JtsGeometry fromText(String wkt) {
    return null;
  }

  @Override
  public ODocument toDoc(JtsGeometry shape) {
    final MultiLineString geom = (MultiLineString) shape.getGeom();

    ODocument doc = new ODocument(getName());
    for (int i = 0; i < geom.getNumGeometries(); i++) {
      final LineString lineString = (LineString) geom.getGeometryN(i);
      doc.field(COORDINATES, new ArrayList<List<List<Double>>>() {
        {
          add(coordinatesFromLineString(lineString));
        }
      });
    }
    return doc;
  }
}
