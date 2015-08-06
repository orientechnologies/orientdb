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
import com.orientechnologies.orient.core.metadata.schema.OSchemaProxy;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.spatial4j.core.context.SpatialContext;
import com.spatial4j.core.shape.Point;
import com.spatial4j.core.shape.Shape;

import java.util.Collection;
import java.util.List;

public class ORectangleShapeBuilder extends OShapeBuilder {
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

    Point[] points = new Point[2];
    int i = 0;
    for (Object o : key.getKeys()) {
      List<Number> numbers = (List<Number>) o;
      double lat = ((Double) OType.convert(numbers.get(0), Double.class)).doubleValue();
      double lng = ((Double) OType.convert(numbers.get(1), Double.class)).doubleValue();
      points[i] = ctx.makePoint(lng, lat);
      i++;
    }
    return ctx.makeRectangle(points[0], points[1]);
  }

  @Override
  public boolean canHandle(OCompositeKey key) {
    boolean canHandle = key.getKeys().size() == 2;
    for (Object o : key.getKeys()) {
      if (!(o instanceof Collection)) {
        canHandle = false;
        break;
      } else if (((Collection) o).size() != 2) {
        canHandle = false;
        break;
      }
    }
    return canHandle;
  }

  @Override
  public void initClazz(ODatabaseDocumentTx db) {

    OSchemaProxy schema = db.getMetadata().getSchema();
    schema.createClass("Rectangle");

  }

  @Override
  public String asText(Shape shape) {
    return null;
  }

  @Override
  public Shape fromText(String wkt) {
    return null;
  }
}
