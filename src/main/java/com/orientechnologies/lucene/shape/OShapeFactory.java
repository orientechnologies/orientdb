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
import com.spatial4j.core.shape.Shape;

import java.util.HashMap;
import java.util.Map;

public class OShapeFactory extends OShapeBuilder {

  private Map<String, OShapeBuilder> factories = new HashMap<String, OShapeBuilder>();

  public static final OShapeFactory  INSTANCE  = new OShapeFactory();

  protected OShapeFactory() {
    registerFactory(new OLineShapeBuilder());
    registerFactory(new OMultiLineShapeBuilder());
    registerFactory(new OPointShapeBuilder());
    // registerFactory(Point.class, new OMultiPointShapeFactory());
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
