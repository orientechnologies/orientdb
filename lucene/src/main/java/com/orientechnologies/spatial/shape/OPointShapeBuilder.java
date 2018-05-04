/**
 * Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * <p>
 * For more information: http://www.orientdb.com
 */
package com.orientechnologies.spatial.shape;

import com.orientechnologies.orient.core.db.ODatabaseInternal;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OProperty;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import org.locationtech.spatial4j.shape.Point;

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
  public void initClazz(ODatabaseInternal db) {

    OSchema schema = db.getMetadata().getSchema();
    OClass point = schema.createAbstractClass(getName(), superClass(db));
    OProperty coordinates = point.createProperty(COORDINATES, OType.EMBEDDEDLIST, OType.DOUBLE);
    coordinates.setMin("2");
    coordinates.setMin("2");
  }

  @Override
  public Point fromDoc(ODocument document) {
    validate(document);
    List<Number> coordinates = document.field(COORDINATES);
    Point point = SHAPE_FACTORY.pointXY(coordinates.get(0).doubleValue(), coordinates.get(1).doubleValue());
    return point;
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
