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
import static com.orientechnologies.spatial.shape.OCoordinateSpaceTransformations.WGS84SpaceRid;
import org.locationtech.spatial4j.shape.Point;
import org.locationtech.spatial4j.shape.Rectangle;

import java.util.ArrayList;
import java.util.List;

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
  public Rectangle fromDoc(ODocument document, Integer srid) {
    validate(document);
    List<Number> coordinates = document.field(COORDINATES);
    double[] coord = {coordinates.get(0).doubleValue(), coordinates.get(1).doubleValue()};
    coord = OCoordinateSpaceTransformations.transform(WGS84SpaceRid, srid, coord);
    Point topLeft = SPATIAL_CONTEXT.makePoint(coord[0], coord[1]);
    
    coord[0] = coordinates.get(2).doubleValue(); coord[1] = coordinates.get(3).doubleValue();
    coord = OCoordinateSpaceTransformations.transform(WGS84SpaceRid, srid, coord);
    Point bottomRight = SPATIAL_CONTEXT.makePoint(coord[0], coord[1]);
    Rectangle rectangle = SPATIAL_CONTEXT.makeRectangle(topLeft, bottomRight);
    
    return rectangle;
  }

  @Override
  public ODocument toDoc(final Rectangle shape, Integer srid) {

    ODocument doc = new ODocument(getName());

    doc.field(COORDINATES, new ArrayList<Double>() {
      {        
        double[] coord = {shape.getMinX(), shape.getMinY()};
        coord = OCoordinateSpaceTransformations.transform(srid, WGS84SpaceRid, coord);
        add(coord[0]);
        add(coord[1]);
        
        coord[0] = shape.getMaxX(); coord[1] = shape.getMaxY();
        coord = OCoordinateSpaceTransformations.transform(srid, WGS84SpaceRid, coord);
        add(coord[0]);
        add(coord[1]);
      }
    });
    return doc;
  }
}
