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

public abstract class OShapeBuilder<T extends Shape> {

  public static final JtsSpatialContext SPATIAL_CONTEXT = JtsSpatialContext.GEO;

  public abstract String getName();

  public abstract OShapeType getType();

  public abstract T makeShape(OCompositeKey key, SpatialContext ctx);

  public abstract boolean canHandle(OCompositeKey key);

  public abstract void initClazz(ODatabaseDocumentTx db);

  public String asText(T shape) {
    return SPATIAL_CONTEXT.getGeometryFrom(shape).toText();
  }

  public String asText(ODocument document) {
    return null;
  }

  public void validate(ODocument doc) {

    if (!doc.getClassName().equals(getName())) {
    }

  }

  public abstract T fromText(String wkt);

}
