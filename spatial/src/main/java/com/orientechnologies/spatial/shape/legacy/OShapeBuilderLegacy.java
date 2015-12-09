package com.orientechnologies.spatial.shape.legacy;

import com.orientechnologies.orient.core.index.OCompositeKey;
import com.spatial4j.core.context.SpatialContext;
import com.spatial4j.core.shape.Shape;

/**
 * Created by Enrico Risa on 23/10/15.
 */
public interface OShapeBuilderLegacy<T extends Shape> {

  public T makeShape(OCompositeKey key, SpatialContext ctx);

  public boolean canHandle(OCompositeKey key);
}
