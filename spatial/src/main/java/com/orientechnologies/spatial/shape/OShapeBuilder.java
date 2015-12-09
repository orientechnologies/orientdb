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
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.spatial4j.core.context.SpatialContext;
import com.spatial4j.core.context.jts.JtsSpatialContext;
import com.spatial4j.core.context.jts.JtsSpatialContextFactory;
import com.spatial4j.core.io.jts.JtsWktShapeParser;
import com.spatial4j.core.shape.Rectangle;
import com.spatial4j.core.shape.Shape;
import com.spatial4j.core.shape.jts.JtsGeometry;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.io.WKBWriter;
import com.vividsolutions.jts.operation.buffer.BufferOp;
import com.vividsolutions.jts.operation.buffer.BufferParameters;

import java.text.ParseException;
import java.util.HashMap;
import java.util.Map;

public abstract class OShapeBuilder<T extends Shape> {

  protected static final JtsSpatialContext SPATIAL_CONTEXT;
  protected static final GeometryFactory   GEOMETRY_FACTORY;

  protected static Map<String, Integer>    capStyles   = new HashMap<String, Integer>();
  protected static Map<String, Integer>    join        = new HashMap<String, Integer>();
  static {

    JtsSpatialContextFactory factory = new JtsSpatialContextFactory();
    factory.geo = true;
    factory.validationRule = JtsWktShapeParser.ValidationRule.none;
    SPATIAL_CONTEXT = new JtsSpatialContext(factory);
    GEOMETRY_FACTORY = SPATIAL_CONTEXT.getGeometryFactory();
    capStyles.put("round", 1);
    capStyles.put("flat", 2);
    capStyles.put("square", 1);

    join.put("round", 1);
    join.put("mitre", 2);
    join.put("bevel", 3);

  }
  public static final String               COORDINATES = "coordinates";
  public static final String               BASE_CLASS  = "OShape";

  public abstract String getName();

  public abstract OShapeType getType();

  public abstract T fromDoc(ODocument doc);

  public T fromObject(Object obj) {
    throw new UnsupportedOperationException();
  }

  public T fromMapGeoJson(Map<String, Object> geoJsonMap) {
    ODocument doc = new ODocument(getName());
    doc.field(COORDINATES, geoJsonMap.get(COORDINATES));
    return fromDoc(doc);
  }

  public abstract void initClazz(ODatabaseDocumentTx db);

  public String asText(T shape) {
    return SPATIAL_CONTEXT.getGeometryFrom(shape).toText();
  }

  public byte[] asBinary(T shape) {
    WKBWriter writer = new WKBWriter();
    Geometry geom = SPATIAL_CONTEXT.getGeometryFrom(shape);
    return writer.write(geom);
  }

  public String asText(ODocument document) {
    return asText(fromDoc(document));
  }

  public String asText(Map<String, Object> geoJson) {
    return asText(fromMapGeoJson(geoJson));
  }

  public String asText(Object object) {
    throw new UnsupportedOperationException();
  }

  public String asGeoJson(T shape) {
    return null;
  }

  public String asGeoJson(ODocument document) {
    return asText(fromDoc(document));
  }

  public void validate(ODocument doc) {

    if (!doc.getClassName().equals(getName())) {
    }

  }

  Geometry toGeometry(Shape shape) {
    return SPATIAL_CONTEXT.getGeometryFrom(shape);
  }

  public JtsGeometry toShape(Geometry geometry) {
    return SPATIAL_CONTEXT.makeShape(geometry);
  }

  protected OClass superClass(ODatabaseDocumentTx db) {
    return db.getMetadata().getSchema().getClass(BASE_CLASS);
  }

  public T fromText(String wkt) throws ParseException {
    T entity = (T) SPATIAL_CONTEXT.getWktShapeParser().parse(wkt);

    if (entity instanceof Rectangle) {
      Geometry geometryFrom = SPATIAL_CONTEXT.getGeometryFrom(entity);
      entity = (T) SPATIAL_CONTEXT.makeShape(geometryFrom);
    }
    return entity;
  }

  public abstract ODocument toDoc(T shape);

  public ODocument toDoc(String wkt) throws ParseException {
    T parsed = fromText(wkt);
    return toDoc(parsed);
  }

  public int getSRID(Shape shape) {
    Geometry geometry = toGeometry(shape);
    return geometry.getSRID();
  }

  public Shape buffer(Shape shape, Double distance, Map<String, Object> params) {
    Geometry geometry = toGeometry(shape);
    BufferParameters parameters = new BufferParameters();
    if (params != null) {
      bindParameters(parameters, params);
    }

    BufferOp ops = new BufferOp(geometry, parameters);
    return toShape(ops.getResultGeometry(distance));
  }

  private void bindParameters(BufferParameters parameters, Map<String, Object> params) {

    bindQuad(parameters, params);

    bindMitre(parameters, params);

    bindCap(parameters, params);

    bindJoin(parameters, params);
  }

  private void bindCap(BufferParameters parameters, Map<String, Object> params) {
    String endCap = (String) params.get("endCap");

    if (endCap != null) {
      Integer style = capStyles.get(endCap);
      if (style != null) {
        parameters.setEndCapStyle(style);
      }
    }

  }

  private void bindJoin(BufferParameters parameters, Map<String, Object> params) {
    String join = (String) params.get("join");
    if (join != null) {
      Integer style = OShapeBuilder.join.get(join);
      if (style != null) {
        parameters.setJoinStyle(style);
      }
    }
  }

  private void bindMitre(BufferParameters parameters, Map<String, Object> params) {
    Number mitre = (Number) params.get("mitre");

    if (mitre != null) {
      parameters.setMitreLimit(mitre.doubleValue());
    }
  }

  private void bindQuad(BufferParameters parameters, Map<String, Object> params) {
    Number quadSegs = (Number) params.get("quadSegs");

    if (quadSegs != null) {
      parameters.setQuadrantSegments(quadSegs.intValue());
    }
  }

  public SpatialContext context() {
    return SPATIAL_CONTEXT;
  }

}
