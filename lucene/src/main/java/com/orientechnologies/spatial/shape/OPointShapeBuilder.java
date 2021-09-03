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
import com.orientechnologies.orient.core.metadata.schema.OProperty;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import java.util.ArrayList;
import java.util.List;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.spatial4j.shape.Point;

public class OPointShapeBuilder extends OShapeBuilder<Point> {

  public static final String NAME = "OPoint";

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
    coordinates.setMax("2");

    if (OGlobalConfiguration.SPATIAL_ENABLE_DIRECT_WKT_READER.getValueAsBoolean()) {
      OClass pointz = schema.createAbstractClass(getName() + "Z", superClass(db));
      OProperty coordinatesz = pointz.createProperty(COORDINATES, OType.EMBEDDEDLIST, OType.DOUBLE);
      coordinatesz.setMin("3");
      coordinatesz.setMax("3");
    }
  }

  @Override
  public Point fromDoc(ODocument document) {
    validate(document);
    List<Number> coordinates = document.field(COORDINATES);
    if (coordinates.size() == 2) {
      return SHAPE_FACTORY.pointXY(
          coordinates.get(0).doubleValue(), coordinates.get(1).doubleValue());
    } else {
      return SHAPE_FACTORY.pointXYZ(
          coordinates.get(0).doubleValue(),
          coordinates.get(1).doubleValue(),
          coordinates.get(2).doubleValue());
    }
  }

  @Override
  public ODocument toDoc(final Point shape) {

    ODocument doc = new ODocument(getName());
    doc.field(
        COORDINATES,
        new ArrayList<Double>() {
          {
            add(shape.getX());
            add(shape.getY());
          }
        });
    return doc;
  }

  @Override
  protected ODocument toDoc(Point parsed, Geometry geometry) {
    if (geometry == null || Double.isNaN(geometry.getCoordinate().getZ())) {
      return toDoc(parsed);
    }

    ODocument doc = new ODocument(getName() + "Z");
    doc.field(
        COORDINATES,
        new ArrayList<Double>() {
          {
            add(geometry.getCoordinate().getX());
            add(geometry.getCoordinate().getY());
            add(geometry.getCoordinate().getZ());
          }
        });
    return doc;
  }

  @Override
  public String asText(ODocument document) {
    if (document.getClassName().equals("OPointZ")) {
      List<Double> coordinates = document.getProperty("coordinates");
      return "POINT Z ("
          + format(coordinates.get(0))
          + " "
          + format(coordinates.get(1))
          + " "
          + format(coordinates.get(2))
          + ")";
    } else {
      return super.asText(document);
    }
  }
}
