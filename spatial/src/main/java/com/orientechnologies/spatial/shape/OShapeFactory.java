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

package com.orientechnologies.spatial.shape;

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.spatial4j.core.shape.Point;
import com.spatial4j.core.shape.Rectangle;
import com.spatial4j.core.shape.Shape;
import com.spatial4j.core.shape.ShapeCollection;
import com.spatial4j.core.shape.jts.JtsGeometry;
import com.vividsolutions.jts.geom.Geometry;

import java.text.ParseException;
import java.util.HashMap;
import java.util.Map;

public class OShapeFactory extends OComplexShapeBuilder {

  private Map<String, OShapeBuilder> factories = new HashMap<String, OShapeBuilder>();

  public static final OShapeFactory  INSTANCE  = new OShapeFactory();

  protected OShapeOperation          operation;

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
  public Shape fromObject(Object obj) {

    if (obj instanceof String) {
      try {
        return fromText((String) obj);
      } catch (ParseException e) {
        e.printStackTrace();
      }
    }
    if (obj instanceof ODocument) {
      return fromDoc((ODocument) obj);
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
    OShapeBuilder oShapeBuilder = factories.get(document.getClassName());
    if (oShapeBuilder != null) {
      return oShapeBuilder.asText(document);
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
    return SPATIAL_CONTEXT.getGeometryFrom(shape);
  }

  public ODocument toDoc(Geometry geometry) {
    if (geometry instanceof com.vividsolutions.jts.geom.Point) {
      com.vividsolutions.jts.geom.Point point = (com.vividsolutions.jts.geom.Point) geometry;
      Point point1 = context().makePoint(point.getX(), point.getY());
      return toDoc(point1);
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
