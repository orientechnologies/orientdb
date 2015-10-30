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

import com.orientechnologies.orient.core.index.OCompositeKey;
import com.spatial4j.core.context.SpatialContext;
import com.spatial4j.core.shape.Point;
import com.spatial4j.core.shape.Rectangle;
import com.spatial4j.core.shape.Shape;

import java.util.HashMap;
import java.util.Map;

public class OShapeFactoryImpl implements OShapeFactory {

  private Map<Class<? extends Shape>, OShapeFactory> factories = new HashMap<Class<? extends Shape>, OShapeFactory>();

  public static OShapeFactoryImpl INSTANCE = new OShapeFactoryImpl();
  protected OShapeFactoryImpl() {
    registerFactory(Point.class, new OPointShapeFactory());
    registerFactory(Rectangle.class, new ORectangleShapeFactory());
    registerFactory(Shape.class, new OPolygonShapeFactory());
  }

  @Override
  public Shape makeShape(OCompositeKey key, SpatialContext ctx) {
    for (OShapeFactory f : factories.values()) {
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

  public void registerFactory(Class<? extends Shape> clazz, OShapeFactory factory) {
    factories.put(clazz, factory);
  }
}
