/*
 * Copyright 2018 OrientDB.
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
package com.orientechnologies.spatial.shape;

import com.orientechnologies.orient.core.db.ODatabaseInternal;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.spatial4j.shape.Shape;
import org.locationtech.spatial4j.shape.ShapeCollection;

import java.util.ArrayList;
import java.util.List;

/**
 * @author marko
 */
public class OGeometryGenericShapeBuilder extends OComplexShapeBuilder<Shape> {

  private static final String GEOMETRIES    = "geometries";
  private static final String IS_COLLECTION = "is_collection";

  protected OShapeFactory shapeFactory;

  public OGeometryGenericShapeBuilder(OShapeFactory shapeFactory) {
    this.shapeFactory = shapeFactory;
  }

  @Override
  public String getName() {
    return "OGeometry";
  }

  @Override
  public OShapeType getType() {
    return OShapeType.GEOMETRY;
  }

  @Override
  public void initClazz(ODatabaseInternal db) {

    OSchema schema = db.getMetadata().getSchema();
    OClass geometry = schema.createAbstractClass(getName(), superClass(db));
    OClass shape = superClass(db);
    geometry.createProperty(GEOMETRIES, OType.EMBEDDEDLIST, shape);
    geometry.createProperty(IS_COLLECTION, OType.BYTE);
  }

  @Override
  public Shape fromDoc(ODocument doc, Integer srid) {
    List<ODocument> geometries = doc.field(GEOMETRIES);
    byte isCollection = doc.field(IS_COLLECTION);

    List<Shape> shapes = new ArrayList<Shape>();

    Geometry[] geoms = new Geometry[geometries.size()];
    for (ODocument geometry : geometries) {
      Shape shape = shapeFactory.fromDoc(geometry, srid);
      shapes.add(shape);
    }

    if (isCollection == 1)
      return new ShapeCollection(shapes, SPATIAL_CONTEXT);
    else
      return shapes.get(0);
  }

  @Override
  public ODocument toDoc(Shape shape, Integer srid) {
    ODocument doc = new ODocument(getName());
    List<ODocument> geometries;
    boolean isCollection = false;
    if (shape instanceof ShapeCollection) {
      ShapeCollection<Shape> shapes = (ShapeCollection) shape;
      geometries = new ArrayList<>(shapes.size());
      for (Shape s : shapes) {
        geometries.add(shapeFactory.toDoc(s, srid));
      }
      isCollection = true;
    } else {
      geometries = new ArrayList<>(1);
      geometries.add(shapeFactory.toDoc(shape, srid));
    }

    doc.field(GEOMETRIES, geometries);
    doc.field(IS_COLLECTION, isCollection ? (byte) 1 : (byte) 0);
    return doc;
  }

}
