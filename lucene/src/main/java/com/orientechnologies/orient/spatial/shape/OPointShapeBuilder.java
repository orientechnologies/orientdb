/*
 * Copyright 2014 Orient Technologies.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.orientechnologies.orient.spatial.shape;

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.index.OCompositeKey;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OProperty;
import com.orientechnologies.orient.core.metadata.schema.OSchemaProxy;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.spatial4j.core.context.SpatialContext;
import com.spatial4j.core.shape.Point;

import java.util.ArrayList;
import java.util.List;

public class OPointShapeBuilder extends OShapeBuilder<Point> {

  public static String NAME = "OPoint";

  @Override
  public String getName() {
    return NAME;
  }

  @Override
  public OShapeType getType() {
    return OShapeType.POINT;
  }

  @Override
  public Point makeShape(OCompositeKey key, SpatialContext ctx) {
    double lat = ((Double) OType.convert(((OCompositeKey) key).getKeys().get(0), Double.class)).doubleValue();
    double lng = ((Double) OType.convert(((OCompositeKey) key).getKeys().get(1), Double.class)).doubleValue();
    return ctx.makePoint(lng, lat);

  }

  @Override
  public boolean canHandle(OCompositeKey key) {

    boolean canHandle = key.getKeys().size() == 2;
    for (Object o : key.getKeys()) {
      if (!(o instanceof Number)) {
        canHandle = false;
        break;
      }
    }
    return canHandle;
  }

  @Override
  public void initClazz(ODatabaseDocumentTx db) {

    OSchemaProxy schema = db.getMetadata().getSchema();
    OClass point = schema.createClass(getName());
    point.setAbstract(true);
    point.addSuperClass(superClass(db));
    OProperty coordinates = point.createProperty(COORDINATES, OType.EMBEDDEDLIST, OType.DOUBLE);
    coordinates.setMin("2");
    coordinates.setMin("2");
  }

  @Override
  public Point fromDoc(ODocument document) {
    validate(document);
    List<Number> coordinates = document.field(COORDINATES);
    Point point = SPATIAL_CONTEXT.makePoint(coordinates.get(0).doubleValue(), coordinates.get(1).doubleValue());
    return point;
  }

  @Override
  public Point fromText(String wkt) {
    return null;
  }

  @Override
  public ODocument toDoc(final Point shape) {

    ODocument doc = new ODocument(getName());
    doc.field(COORDINATES, new ArrayList<Double>() {
      {
        add(shape.getX());
        add(shape.getY());
      }
    });
    return doc;
  }
}
