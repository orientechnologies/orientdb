package com.orientechnologies.spatial.shape.legacy;

import com.orientechnologies.orient.core.index.OCompositeKey;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.spatial4j.core.context.SpatialContext;
import com.spatial4j.core.shape.Point;
import com.spatial4j.core.shape.Rectangle;

import java.util.Collection;
import java.util.List;

/**
 * Created by Enrico Risa on 23/10/15.
 */
public class ORectangleLegacyBuilder implements OShapeBuilderLegacy<Rectangle> {

  @Override
  public Rectangle makeShape(OCompositeKey key, SpatialContext ctx) {

    Point[] points = new Point[2];
    int i = 0;

    for (Object o : key.getKeys()) {
      List<Number> numbers = (List<Number>) o;
      double lat = ((Double) OType.convert(numbers.get(0), Double.class)).doubleValue();
      double lng = ((Double) OType.convert(numbers.get(1), Double.class)).doubleValue();
      points[i] = ctx.makePoint(lng, lat);
      i++;
    }

    Point lowerLeft = points[0];
    Point topRight = points[1];
    if (lowerLeft.getX() > topRight.getX()) {
      double x = lowerLeft.getX();
      lowerLeft = ctx.makePoint(topRight.getX(), lowerLeft.getY());
      topRight = ctx.makePoint(x, topRight.getY());
    }
    return ctx.makeRectangle(lowerLeft, topRight);
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
}
