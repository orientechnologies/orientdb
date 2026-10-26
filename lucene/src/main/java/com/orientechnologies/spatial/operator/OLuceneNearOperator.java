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
import com.orientechnologies.spatial.shape.OShapeFactory;
import java.util.List;
import java.util.Map;
import org.locationtech.spatial4j.distance.DistanceUtils;
import org.locationtech.spatial4j.shape.Circle;
import org.locationtech.spatial4j.shape.Point;
import org.locationtech.spatial4j.shape.Shape;
import org.locationtech.spatial4j.shape.SpatialRelation;

public class OLuceneNearOperator extends OQueryTargetOperator {

  private OShapeFactory factory = OShapeFactory.INSTANCE;

  public OLuceneNearOperator() {
    super("NEAR", 5, false);
  }

  @Override
  public boolean evaluate(Object iLeft, Object iRight, OCommandContext ctx) {

    List<Number> left = (List<Number>) iLeft;

    double lat = left.get(0).doubleValue();
    double lon = left.get(1).doubleValue();

    Shape shape = factory.context().getShapeFactory().pointXY(lon, lat);
    List<Number> right = (List<Number>) iRight;

    double lat1 = right.get(0).doubleValue();
    double lon1 = right.get(1).doubleValue();
    Shape shape1 = factory.context().getShapeFactory().pointXY(lon1, lat1);

    Map map = (Map) right.get(2);
    double distance = 0;

    Number n = (Number) map.get("maxDistance");
    if (n != null) {
      distance = n.doubleValue();
    }
    Point p = (Point) shape1;
    Circle circle =
        factory
            .context()
            .getShapeFactory()
            .circle(
                p.getX(),
                p.getY(),
                DistanceUtils.dist2Degrees(distance, DistanceUtils.EARTH_MEAN_RADIUS_KM));
    double docDistDEG = factory.context().getDistCalc().distance((Point) shape, p);
    final double docDistInKM =
        DistanceUtils.degrees2Dist(docDistDEG, DistanceUtils.EARTH_EQUATORIAL_RADIUS_KM);
    ctx.setVariable("distance", docDistInKM);
    return shape.relate(circle) == SpatialRelation.WITHIN;
  }

  @Override
  public String getSyntax() {
    return "<left> NEAR[(<begin-deep-level> [,<maximum-deep-level> [,<fields>]] )] ( <conditions>"
        + " )";
  }
}
