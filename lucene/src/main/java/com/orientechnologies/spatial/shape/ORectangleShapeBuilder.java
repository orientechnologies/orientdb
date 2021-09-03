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
import com.orientechnologies.orient.core.metadata.schema.OProperty;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import java.util.ArrayList;
import java.util.List;
import org.locationtech.spatial4j.shape.Point;
import org.locationtech.spatial4j.shape.Rectangle;

public class ORectangleShapeBuilder extends OShapeBuilder<Rectangle> {

  public static final String NAME = "ORectangle";

  @Override
  public String getName() {
    return NAME;
  }

  @Override
  public OShapeType getType() {
    return OShapeType.RECTANGLE;
  }

  @Override
  public void initClazz(ODatabaseInternal db) {

    OSchema schema = db.getMetadata().getSchema();
    OClass rectangle = schema.createAbstractClass(getName(), superClass(db));
    OProperty coordinates = rectangle.createProperty(COORDINATES, OType.EMBEDDEDLIST, OType.DOUBLE);
    coordinates.setMin("4");
    coordinates.setMin("4");
  }

  @Override
  public Rectangle fromDoc(ODocument document) {
    validate(document);
    List<Number> coordinates = document.field(COORDINATES);

    Point topLeft =
        SPATIAL_CONTEXT.makePoint(
            coordinates.get(0).doubleValue(), coordinates.get(1).doubleValue());
    Point bottomRight =
        SPATIAL_CONTEXT.makePoint(
            coordinates.get(2).doubleValue(), coordinates.get(3).doubleValue());
    Rectangle rectangle = SPATIAL_CONTEXT.makeRectangle(topLeft, bottomRight);
    return rectangle;
  }

  @Override
  public ODocument toDoc(final Rectangle shape) {

    ODocument doc = new ODocument(getName());

    doc.field(
        COORDINATES,
        new ArrayList<Double>() {
          {
            add(shape.getMinX());
            add(shape.getMinY());
            add(shape.getMaxX());
            add(shape.getMaxY());
          }
        });
    return doc;
  }
}
