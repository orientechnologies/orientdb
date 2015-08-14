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

package com.orientechnologies.lucene.shape;

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.index.OCompositeKey;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.spatial4j.core.context.SpatialContext;
import com.spatial4j.core.shape.Point;
import com.spatial4j.core.shape.Rectangle;
import com.spatial4j.core.shape.Shape;
import com.spatial4j.core.shape.ShapeCollection;
import com.spatial4j.core.shape.jts.JtsGeometry;
import com.spatial4j.core.shape.jts.JtsPoint;
import com.vividsolutions.jts.geom.*;

import java.util.HashMap;
import java.util.Map;

public class OShapeFactory extends OComplexShapeBuilder {

  private Map<String, OShapeBuilder> factories = new HashMap<String, OShapeBuilder>();

  public static final OShapeFactory  INSTANCE  = new OShapeFactory();

  protected OShapeFactory() {
    registerFactory(new OLineStringShapeBuilder());
    registerFactory(new OMultiLineStringShapeBuilder());
    registerFactory(new OPointShapeBuilder());
    registerFactory(new OMultiPointShapeBuilder());
    registerFactory(new ORectangleShapeBuilder());
    registerFactory(new OPolygonShapeBuilder());
    registerFactory(new OMultiPolygonShapeBuilder());
  }

  @Override
  public String getName() {
    return null;
  }

  @Override
  public OShapeType getType() {
    return null;
  }

  @Override
  public Shape makeShape(OCompositeKey key, SpatialContext ctx) {
    for (OShapeBuilder f : factories.values()) {
      if (f.canHandle(key)) {
        return f.makeShape(key, ctx);
      }
    }
    return null;
  }

  @Override
  public boolean canHandle(OCompositeKey key) {
    return false;
  }

  @Override
  public void initClazz(ODatabaseDocumentTx db) {
    for (OShapeBuilder f : factories.values()) {
      f.initClazz(db);
    }
  }

  @Override
  public Shape fromDoc(ODocument document) {
    OShapeBuilder oShapeBuilder = factories.get(document.getClassName());
    if (oShapeBuilder != null) {
      return oShapeBuilder.fromDoc(document);
    }
    // TODO handle exception shape not found
    return null;
  }

  @Override
  public String asText(ODocument document) {
    OShapeBuilder oShapeBuilder = factories.get(document.getClassName());
    if (oShapeBuilder != null) {
      return oShapeBuilder.asText(document);
    }
    // TODO handle exception shape not found
    return null;
  }

  @Override
  public String asText(Shape shape) {
    return null;
  }

  @Override
  public Shape fromText(String wkt) {
    return null;
  }

  @Override
  public ODocument toDoc(Shape shape) {

    ODocument doc = null;
    if (Point.class.isAssignableFrom(shape.getClass())) {
      doc = factories.get(Point.class.getSimpleName()).toDoc(shape);
    } else if (Rectangle.class.isAssignableFrom(shape.getClass())) {
      doc = factories.get(Rectangle.class.getSimpleName()).toDoc(shape);
    } else if (JtsGeometry.class.isAssignableFrom(shape.getClass())) {
      JtsGeometry geometry = (JtsGeometry) shape;
      Geometry geom = geometry.getGeom();
      doc = factories.get(geom.getClass().getSimpleName()).toDoc(shape);

    } else if (ShapeCollection.class.isAssignableFrom(shape.getClass())) {
      ShapeCollection collection = (ShapeCollection) shape;

      if (isMultiPolygon(collection)) {
        doc = factories.get("MultiPolygon").toDoc(createMultiPolygon(collection));
      } else if (isMultiPoint(collection)) {
        doc = factories.get("MultiPoint").toDoc(createMultiPoint(collection));
      } else if (isMultiLine(collection)) {
        doc = factories.get("MultiLineString").toDoc(createMultiLine(collection));
      }
    }
    return doc;
  }

  protected JtsGeometry createMultiPoint(ShapeCollection<JtsPoint> geometries) {

    Coordinate[] points = new Coordinate[geometries.size()];

    int i = 0;

    for (JtsPoint geometry : geometries) {
      points[i] = new Coordinate(geometry.getX(), geometry.getY());
      i++;
    }

    MultiPoint multiPoints = GEOMETRY_FACTORY.createMultiPoint(points);

    return SPATIAL_CONTEXT.makeShape(multiPoints);
  }

  protected JtsGeometry createMultiLine(ShapeCollection<JtsGeometry> geometries) {

    LineString[] multiLineString = new LineString[geometries.size()];

    int i = 0;

    for (JtsGeometry geometry : geometries) {
      multiLineString[i] = (LineString) geometry.getGeom();
      i++;
    }

    MultiLineString multiPoints = GEOMETRY_FACTORY.createMultiLineString(multiLineString);

    return SPATIAL_CONTEXT.makeShape(multiPoints);
  }

  protected JtsGeometry createMultiPolygon(ShapeCollection<JtsGeometry> geometries) {

    Polygon[] polygons = new Polygon[geometries.size()];

    int i = 0;

    for (JtsGeometry geometry : geometries) {
      polygons[i] = (Polygon) geometry.getGeom();
      i++;
    }

    MultiPolygon multiPolygon = GEOMETRY_FACTORY.createMultiPolygon(polygons);

    return SPATIAL_CONTEXT.makeShape(multiPolygon);
  }

  protected boolean isMultiPolygon(ShapeCollection<Shape> collection) {

    boolean isMultiPolygon = true;
    for (Shape shape : collection) {

      if (!isPolygon(shape)) {
        isMultiPolygon = false;
        break;
      }
    }
    return isMultiPolygon;
  }

  protected boolean isMultiPoint(ShapeCollection<Shape> collection) {

    boolean isMultipoint = true;
    for (Shape shape : collection) {

      if (!isPoint(shape)) {
        isMultipoint = false;
        break;
      }
    }
    return isMultipoint;
  }

  protected boolean isMultiLine(ShapeCollection<Shape> collection) {

    boolean isMultipoint = true;
    for (Shape shape : collection) {

      if (!isLineString(shape)) {
        isMultipoint = false;
        break;
      }
    }
    return isMultipoint;
  }

  private boolean isLineString(Shape shape) {
    if (shape instanceof JtsGeometry) {
      Geometry geom = ((JtsGeometry) shape).getGeom();
      return geom instanceof LineString;
    }
    return false;
  }

  protected boolean isPoint(Shape shape) {
    return shape instanceof Point;
  }

  protected boolean isPolygon(Shape shape) {

    if (shape instanceof JtsGeometry) {
      Geometry geom = ((JtsGeometry) shape).getGeom();
      return geom instanceof Polygon;
    }
    return false;
  }

  @Override
  public Shape fromMapGeoJson(Map geoJsonMap) {
    OShapeBuilder oShapeBuilder = factories.get(geoJsonMap.get("type"));
    if (oShapeBuilder != null) {
      return oShapeBuilder.fromMapGeoJson(geoJsonMap);
    }
    // TODO handle exception shape not found
    return null;
  }

  public void registerFactory(OShapeBuilder factory) {
    factories.put(factory.getName(), factory);
  }
}
