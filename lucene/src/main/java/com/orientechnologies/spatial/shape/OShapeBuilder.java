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
import com.orientechnologies.orient.core.record.impl.ODocument;
import java.io.IOException;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.text.ParseException;
import java.util.HashMap;
import java.util.Map;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.io.WKBWriter;
import org.locationtech.jts.io.WKTReader;
import org.locationtech.jts.operation.buffer.BufferOp;
import org.locationtech.jts.operation.buffer.BufferParameters;
import org.locationtech.spatial4j.context.SpatialContext;
import org.locationtech.spatial4j.context.jts.JtsSpatialContext;
import org.locationtech.spatial4j.context.jts.JtsSpatialContextFactory;
import org.locationtech.spatial4j.context.jts.ValidationRule;
import org.locationtech.spatial4j.shape.Rectangle;
import org.locationtech.spatial4j.shape.Shape;
import org.locationtech.spatial4j.shape.jts.JtsGeometry;
import org.locationtech.spatial4j.shape.jts.JtsShapeFactory;

public abstract class OShapeBuilder<T extends Shape> {

  public static final String COORDINATES = "coordinates";
  public static final String BASE_CLASS = "OShape";
  protected static final JtsSpatialContext SPATIAL_CONTEXT;
  protected static final GeometryFactory GEOMETRY_FACTORY;
  protected static final JtsShapeFactory SHAPE_FACTORY;
  protected static final WKTReader wktReader = new WKTReader();
  private static final Map<String, Integer> capStyles = new HashMap<String, Integer>();
  private static final Map<String, Integer> join = new HashMap<String, Integer>();
  private static final DecimalFormat doubleFormat;

  static {
    JtsSpatialContextFactory factory = new JtsSpatialContextFactory();
    factory.geo = true;
    factory.validationRule = ValidationRule.none;

    SPATIAL_CONTEXT = new JtsSpatialContext(factory);

    SHAPE_FACTORY = new JtsShapeFactory(SPATIAL_CONTEXT, factory);
    GEOMETRY_FACTORY = SHAPE_FACTORY.getGeometryFactory();
    capStyles.put("round", 1);
    capStyles.put("flat", 2);
    capStyles.put("square", 1);

    join.put("round", 1);
    join.put("mitre", 2);
    join.put("bevel", 3);

    DecimalFormatSymbols sym = new DecimalFormatSymbols();
    sym.setDecimalSeparator('.');
    doubleFormat = new DecimalFormat("0", sym);
    doubleFormat.setMaximumFractionDigits(16);
  }

  public synchronized String format(double value) {
    if (Double.isNaN(value)) {
      return "NaN";
    } else if (Double.isInfinite(value)) {
      if (value > 0d) {
        return "Inf";
      } else {
        return "-Inf";
      }
    } else {
      return doubleFormat.format(value);
    }
  }

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

  public abstract void initClazz(ODatabaseInternal db);

  public String asText(T shape) {
    return SHAPE_FACTORY.getGeometryFrom(shape).toText();
  }

  public byte[] asBinary(T shape) {
    WKBWriter writer = new WKBWriter();

    Geometry geom = SHAPE_FACTORY.getGeometryFrom(shape);
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
    return SPATIAL_CONTEXT.getFormats().getGeoJsonWriter().toString(shape);
  }

  public String asGeoJson(ODocument document) {
    return asGeoJson(fromDoc(document));
  }

  public ODocument fromGeoJson(String geoJson) throws IOException, ParseException {
    Shape shape = SPATIAL_CONTEXT.getFormats().getGeoJsonReader().read(geoJson);
    return toDoc((T) shape);
  }

  public void validate(ODocument doc) {}

  Geometry toGeometry(Shape shape) {
    return SHAPE_FACTORY.getGeometryFrom(shape);
  }

  public JtsGeometry toShape(Geometry geometry) {
    return SHAPE_FACTORY.makeShape(geometry);
  }

  protected OClass superClass(ODatabaseInternal db) {
    return db.getMetadata().getSchema().getClass(BASE_CLASS);
  }

  public T fromText(String wkt) throws ParseException, org.locationtech.jts.io.ParseException {
    Object entity = (T) SPATIAL_CONTEXT.getWktShapeParser().parse(wkt);

    if (entity instanceof Rectangle) {
      Geometry geometryFrom = SHAPE_FACTORY.getGeometryFrom((Shape) entity);
      entity = (T) SHAPE_FACTORY.makeShape(geometryFrom);
    }
    return (T) entity;
  }

  public abstract ODocument toDoc(T shape);

  protected ODocument toDoc(T parsed, Geometry geometry) {
    if (geometry == null || Double.isNaN(geometry.getCoordinates()[0].getZ())) {
      return toDoc(parsed);
    }
    throw new IllegalArgumentException("Invalid shape");
  }

  public ODocument toDoc(String wkt) throws ParseException, org.locationtech.jts.io.ParseException {
    T parsed = fromText(wkt);
    return toDoc(
        parsed,
        OGlobalConfiguration.SPATIAL_ENABLE_DIRECT_WKT_READER.getValueAsBoolean()
            ? wktReader.read(wkt)
            : null);
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
