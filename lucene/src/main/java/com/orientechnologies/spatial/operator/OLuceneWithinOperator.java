/**
 * Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
 *
 * <p>Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * <p>For more information: http://www.orientdb.com
 */
package com.orientechnologies.spatial.operator;

import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.sql.operator.OQueryTargetOperator;
import com.orientechnologies.spatial.collections.OSpatialCompositeKey;
import com.orientechnologies.spatial.shape.legacy.OShapeBuilderLegacy;
import com.orientechnologies.spatial.shape.legacy.OShapeBuilderLegacyImpl;
import java.util.List;
import org.locationtech.spatial4j.context.SpatialContext;
import org.locationtech.spatial4j.shape.Shape;
import org.locationtech.spatial4j.shape.SpatialRelation;

public class OLuceneWithinOperator extends OQueryTargetOperator {

  private OShapeBuilderLegacy<Shape> shapeFactory = OShapeBuilderLegacyImpl.INSTANCE;

  public OLuceneWithinOperator() {
    super("WITHIN", 5, false);
  }

  @Override
  public boolean evaluate(Object iLeft, Object iRight, OCommandContext ctx) {
    List<Number> left = (List<Number>) iLeft;

    double lat = left.get(0).doubleValue();
    double lon = left.get(1).doubleValue();

    Shape shape = SpatialContext.GEO.getShapeFactory().pointXY(lon, lat);

    Shape shape1 =
        shapeFactory.makeShape(new OSpatialCompositeKey((List<?>) iRight), SpatialContext.GEO);

    return shape.relate(shape1) == SpatialRelation.WITHIN;
  }
}
