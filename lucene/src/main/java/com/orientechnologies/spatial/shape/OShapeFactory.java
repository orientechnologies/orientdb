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
import com.orientechnologies.orient.core.record.OElement;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.executor.OResult;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.spatial4j.shape.Point;
import org.locationtech.spatial4j.shape.Rectangle;
import org.locationtech.spatial4j.shape.Shape;
import org.locationtech.spatial4j.shape.ShapeCollection;
import org.locationtech.spatial4j.shape.jts.JtsGeometry;

public class OShapeFactory extends OComplexShapeBuilder {

  public static final OShapeFactory INSTANCE = new OShapeFactory();

  protected OShapeOperation operation;

  private Map<String, OShapeBuilder> factories = new HashMap<String, OShapeBuilder>();

  protected OShapeFactory() {
    operation = new OShapeOperationImpl(this);
    registerFactory(new OLineStringShapeBuilder());
    registerFactory(new OMultiLineStringShapeBuilder());
    registerFactory(new OPointShapeBuilder());
    registerFactory(new OMultiPointShapeBuilder());
    registerFactory(new ORectangleShapeBuilder());
    registerFactory(new OPolygonShapeBuilder());
    registerFactory(new OMultiPolygonShapeBuilder());
    registerFactory(new OGeometryCollectionShapeBuilder(this));
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
  public void initClazz(ODatabaseInternal db) {
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
  public Shape fromObject(Object obj) {

    if (obj instanceof String) {
      try {
        return fromText((String) obj);
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
    if (obj instanceof ODocument) {
      return fromDoc((ODocument) obj);
    }
    if (obj instanceof OResult) {
      OElement oElement = ((OResult) obj).toElement();
      return fromDoc((ODocument) oElement);
    }
    if (obj instanceof Map) {
      Map map = (Map) ((Map) obj).get("shape");
      if (map == null) {
        map = (Map) obj;
      }
      return fromMapGeoJson(map);
    }
    return null;
  }

  @Override
  public String asText(ODocument document) {
    String className = document.getClassName();
    OShapeBuilder oShapeBuilder = factories.get(className);
    if (oShapeBuilder != null) {
      return oShapeBuilder.asText(document);
    } else if (className.endsWith("Z")) {
      oShapeBuilder = factories.get(className.substring(0, className.length() - 1));
      if (oShapeBuilder != null) {
        return oShapeBuilder.asText(document);
      }
    }

    // TODO handle exception shape not found
    return null;
  }

  @Override
  public String asText(Object obj) {

    if (obj instanceof ODocument) {
      return asText((ODocument) obj);
    }
    if (obj instanceof Map) {
      Map map = (Map) ((Map) obj).get("shape");
      if (map == null) {
        map = (Map) obj;
      }
      return asText(map);
    }
    return null;
  }

  public byte[] asBinary(Object obj) {

    if (obj instanceof ODocument) {
      Shape shape = fromDoc((ODocument) obj);
      return asBinary(shape);
    }
    if (obj instanceof Map) {
      Map map = (Map) ((Map) obj).get("shape");
      if (map == null) {
        map = (Map) obj;
      }
      Shape shape = fromMapGeoJson(map);

      return asBinary(shape);
    }
    throw new IllegalArgumentException("Error serializing to binary " + obj);
  }

  @Override
  public ODocument toDoc(Shape shape) {

    // TODO REFACTOR
    ODocument doc = null;
    if (Point.class.isAssignableFrom(shape.getClass())) {
      doc = factories.get(OPointShapeBuilder.NAME).toDoc(shape);
    } else if (Rectangle.class.isAssignableFrom(shape.getClass())) {
      doc = factories.get(ORectangleShapeBuilder.NAME).toDoc(shape);
    } else if (JtsGeometry.class.isAssignableFrom(shape.getClass())) {
      JtsGeometry geometry = (JtsGeometry) shape;
      Geometry geom = geometry.getGeom();
      doc = factories.get("O" + geom.getClass().getSimpleName()).toDoc(shape);

    } else if (ShapeCollection.class.isAssignableFrom(shape.getClass())) {
      ShapeCollection collection = (ShapeCollection) shape;

      if (isMultiPolygon(collection)) {
        doc = factories.get("OMultiPolygon").toDoc(createMultiPolygon(collection));
      } else if (isMultiPoint(collection)) {
        doc = factories.get("OMultiPoint").toDoc(createMultiPoint(collection));
      } else if (isMultiLine(collection)) {
        doc = factories.get("OMultiLineString").toDoc(createMultiLine(collection));
      } else {
        doc = factories.get("OGeometryCollection").toDoc(shape);
      }
    }
    return doc;
  }

  @Override
  protected ODocument toDoc(Shape shape, Geometry geometry) {
    if (Point.class.isAssignableFrom(shape.getClass())) {
      return factories.get(OPointShapeBuilder.NAME).toDoc(shape, geometry);
    } else if (geometry != null && "LineString".equals(geometry.getClass().getSimpleName())) {
      return factories.get("OLineString").toDoc(shape, geometry);
    } else if (geometry != null && "MultiLineString".equals(geometry.getClass().getSimpleName())) {
      return factories.get("OMultiLineString").toDoc(shape, geometry);
    } else if (geometry != null && "Polygon".equals(geometry.getClass().getSimpleName())) {
      return factories.get("OPolygon").toDoc(shape, geometry);
    } else {
      return toDoc(shape);
    }
  }

  @Override
  public Shape fromMapGeoJson(Map geoJsonMap) {
    OShapeBuilder oShapeBuilder = factories.get(geoJsonMap.get("type"));

    if (oShapeBuilder == null) {
      oShapeBuilder = factories.get(geoJsonMap.get("@class"));
    }
    if (oShapeBuilder != null) {
      return oShapeBuilder.fromMapGeoJson(geoJsonMap);
    }
    throw new IllegalArgumentException("Invalid map");
    // TODO handle exception shape not found
  }

  public Geometry toGeometry(Shape shape) {
    if (shape instanceof ShapeCollection) {
      ShapeCollection<Shape> shapes = (ShapeCollection<Shape>) shape;
      Geometry[] geometries = new Geometry[shapes.size()];
      int i = 0;
      for (Shape shapeItem : shapes) {
        geometries[i] = SPATIAL_CONTEXT.getGeometryFrom(shapeItem);
        i++;
      }
      return GEOMETRY_FACTORY.createGeometryCollection(geometries);
    } else {
      return SPATIAL_CONTEXT.getGeometryFrom(shape);
    }
  }

  public ODocument toDoc(Geometry geometry) {
    if (geometry instanceof org.locationtech.jts.geom.Point) {
      org.locationtech.jts.geom.Point point = (org.locationtech.jts.geom.Point) geometry;
      Point point1 = context().makePoint(point.getX(), point.getY());
      return toDoc(point1);
    }
    if (geometry instanceof org.locationtech.jts.geom.GeometryCollection) {
      org.locationtech.jts.geom.GeometryCollection gc =
          (org.locationtech.jts.geom.GeometryCollection) geometry;
      List<Shape> shapes = new ArrayList<Shape>();
      for (int i = 0; i < gc.getNumGeometries(); i++) {
        Geometry geo = gc.getGeometryN(i);
        Shape shape = null;
        if (geo instanceof org.locationtech.jts.geom.Point) {
          org.locationtech.jts.geom.Point point = (org.locationtech.jts.geom.Point) geo;
          shape = context().makePoint(point.getX(), point.getY());
        } else {
          shape = SPATIAL_CONTEXT.makeShape(geo);
        }
        shapes.add(shape);
      }
      return toDoc(new ShapeCollection<Shape>(shapes, SPATIAL_CONTEXT));
    }
    return toDoc(SPATIAL_CONTEXT.makeShape(geometry));
  }

  public OShapeOperation operation() {
    return operation;
  }

  public void registerFactory(OShapeBuilder factory) {
    factories.put(factory.getName(), factory);
  }
}
