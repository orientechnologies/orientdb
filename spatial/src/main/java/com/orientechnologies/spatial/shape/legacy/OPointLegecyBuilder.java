package com.orientechnologies.spatial.shape.legacy;

import com.orientechnologies.orient.core.index.OCompositeKey;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.spatial4j.core.context.SpatialContext;
import com.spatial4j.core.shape.Point;

/**
 * Created by Enrico Risa on 23/10/15.
 */
public class OPointLegecyBuilder implements OShapeBuilderLegacy<Point> {

  @Override
  public Point makeShape(OCompositeKey key, SpatialContext ctx) {
    double lat = ((Double) OType.convert(((OCompositeKey) key).getKeys().get(0), Double.class)).doubleValue();
    double lng = ((Double) OType.convert(((OCompositeKey) key).getKeys().get(1), Double.class)).doubleValue();
    return ctx.makePoint(lng, lat);

  }

  @Override
  public boolean canHandle(OCompositeKey key) {

    boolean canHandle = key.getKeys().size() == 2;
    for (Object o : key.getKeys()) {
      if (!(o instanceof Number)) {
        canHandle = false;
        break;
      }
    }
    return canHandle;
  }
}
