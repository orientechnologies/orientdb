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

package com.orientechnologies.spatial.shape;

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OSchemaProxy;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.spatial4j.core.shape.Shape;
import com.spatial4j.core.shape.ShapeCollection;
import com.vividsolutions.jts.geom.Geometry;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by Enrico Risa on 17/08/15.
 */
public class OGeometryCollectionShapeBuilder extends OComplexShapeBuilder<ShapeCollection<Shape>> {

  protected OShapeFactory shapeFactory;

  public OGeometryCollectionShapeBuilder(OShapeFactory shapeFactory) {
    this.shapeFactory = shapeFactory;
  }

  @Override
  public String getName() {
    return "OGeometryCollection";
  }

  @Override
  public OShapeType getType() {
    return OShapeType.GEOMETRYCOLLECTION;
  }

  @Override
  public ShapeCollection<Shape> fromDoc(ODocument doc) {

    List<ODocument> geometries = doc.field("geometries");

    List<Shape> shapes = new ArrayList<Shape>();

    Geometry[] geoms = new Geometry[geometries.size()];
    for (ODocument geometry : geometries) {
      Shape shape = shapeFactory.fromDoc(geometry);
      shapes.add(shape);
    }

    return new ShapeCollection(shapes, SPATIAL_CONTEXT);
  }

  @Override
  public void initClazz(ODatabaseDocumentTx db) {

    OSchemaProxy schema = db.getMetadata().getSchema();
    OClass polygon = schema.createClass(getName());
    polygon.setAbstract(true);
    OClass shape = superClass(db);
    polygon.addSuperClass(shape);
    polygon.createProperty("geometries", OType.EMBEDDEDLIST, shape);
  }

  @Override
  public String asText(ShapeCollection<Shape> shapes) {

    Geometry[] geometries = new Geometry[shapes.size()];
    int i = 0;
    for (Shape shape : shapes) {
      geometries[i] = SPATIAL_CONTEXT.getGeometryFrom(shape);
      i++;
    }
    return GEOMETRY_FACTORY.createGeometryCollection(geometries).toText();
  }

  @Override
  public ODocument toDoc(ShapeCollection<Shape> shapes) {

    ODocument doc = new ODocument(getName());
    List<ODocument> geometries = new ArrayList<ODocument>(shapes.size());
    for (Shape s : shapes) {
      geometries.add(shapeFactory.toDoc(s));
    }
    doc.field("geometries", geometries);
    return doc;
  }
}
