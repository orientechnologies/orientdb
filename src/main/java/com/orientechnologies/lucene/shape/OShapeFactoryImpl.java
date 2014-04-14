package com.orientechnologies.lucene.shape;

import com.orientechnologies.lucene.collections.OSpatialCompositeKey;
import com.orientechnologies.orient.core.index.OCompositeKey;
import com.spatial4j.core.context.SpatialContext;
import com.spatial4j.core.shape.Point;
import com.spatial4j.core.shape.Shape;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by enricorisa on 08/04/14.
 */
public class OShapeFactoryImpl implements OShapeFactory {

  private Map<Class<? extends Shape>, OShapeFactory> factories = new HashMap<>();

  public OShapeFactoryImpl() {
    registerFactory(Point.class, new OPointShapeFactory());
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
