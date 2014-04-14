package com.orientechnologies.lucene.shape;

import com.orientechnologies.orient.core.index.OCompositeKey;
import com.spatial4j.core.context.SpatialContext;
import com.spatial4j.core.shape.Shape;

/**
 * Created by enricorisa on 08/04/14.
 */
public class ORectangleShapeFactory implements OShapeFactory {
  @Override
  public Shape makeShape(OCompositeKey key, SpatialContext ctx) {
    return null;
  }

  @Override
  public boolean canHandle(OCompositeKey key) {
    return false;
  }
}
