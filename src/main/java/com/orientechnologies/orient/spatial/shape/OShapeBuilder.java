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
package com.orientechnologies.orient.spatial.shape;

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.index.OCompositeKey;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.spatial4j.core.context.SpatialContext;
import com.spatial4j.core.context.jts.JtsSpatialContext;
import com.spatial4j.core.shape.Shape;
import com.spatial4j.core.shape.jts.JtsGeometry;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;

import java.text.ParseException;
import java.util.Map;

public abstract class OShapeBuilder<T extends Shape> {

  public static final JtsSpatialContext SPATIAL_CONTEXT  = JtsSpatialContext.GEO;
  public static final GeometryFactory   GEOMETRY_FACTORY = JtsSpatialContext.GEO.getGeometryFactory();
  public static final String            COORDINATES      = "coordinates";
  public static final String            BASE_CLASS       = "Shape";

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

  @Deprecated
  public T makeShape(OCompositeKey key, SpatialContext ctx) {
    throw new UnsupportedOperationException();
  };

  @Deprecated
  public boolean canHandle(OCompositeKey key) {
    return false;
  };

  public abstract void initClazz(ODatabaseDocumentTx db);

  public String asText(T shape) {
    return SPATIAL_CONTEXT.getGeometryFrom(shape).toText();
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

  public JtsGeometry toShape(Geometry geometry) {
    // dateline180Check is false because ElasticSearch does it's own dateline wrapping
    JtsGeometry jtsGeometry = new JtsGeometry(geometry, SPATIAL_CONTEXT, false, false);
    // if (autoValidateJtsGeometry)
    // jtsGeometry.validate();
    // if (autoIndexJtsGeometry)
    // jtsGeometry.index();
    return jtsGeometry;
  }

  protected OClass superClass(ODatabaseDocumentTx db) {
    return db.getMetadata().getSchema().getClass(BASE_CLASS);
  }

  public T fromText(String wkt) throws ParseException {
    return (T) SPATIAL_CONTEXT.getWktShapeParser().parse(wkt);
  }

  public abstract ODocument toDoc(T shape);

  public ODocument toDoc(String wkt) throws ParseException {
    T parsed = (T) SPATIAL_CONTEXT.getWktShapeParser().parse(wkt);
    return toDoc(parsed);
  }

}
