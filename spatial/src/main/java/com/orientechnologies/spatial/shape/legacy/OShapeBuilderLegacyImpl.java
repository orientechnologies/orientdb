package com.orientechnologies.spatial.shape.legacy;

import com.orientechnologies.orient.core.index.OCompositeKey;
import com.spatial4j.core.context.SpatialContext;
import com.spatial4j.core.shape.Shape;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by Enrico Risa on 23/10/15.
 */
public class OShapeBuilderLegacyImpl implements OShapeBuilderLegacy<Shape> {

  List<OShapeBuilderLegacy>             builders = new ArrayList<OShapeBuilderLegacy>();

  public static OShapeBuilderLegacyImpl INSTANCE = new OShapeBuilderLegacyImpl();

  protected OShapeBuilderLegacyImpl() {
    builders.add(new OPointLegecyBuilder());
    builders.add(new ORectangleLegacyBuilder());
  }

  @Override
  public Shape makeShape(OCompositeKey key, SpatialContext ctx) {
    for (OShapeBuilderLegacy f : builders) {
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
}
