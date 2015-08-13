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

  public abstract String getName();

  public abstract OShapeType getType();

  public T fromDoc(ODocument doc) {
    return null;
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
    throw new UnsupportedOperationException();
  };

  public abstract void initClazz(ODatabaseDocumentTx db);

  public String asText(T shape) {
    return SPATIAL_CONTEXT.getGeometryFrom(shape).toText();
  }

  public String asText(ODocument document) {
    return asText(fromDoc(document));
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

  public abstract T fromText(String wkt);

  public abstract ODocument toDoc(T shape);

  public ODocument toDoc(String wkt) throws ParseException {
    T parsed = (T) SPATIAL_CONTEXT.getWktShapeParser().parse(wkt);
    return toDoc(parsed);
  }

}
