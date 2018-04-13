/**
 * Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * <p>
 * For more information: http://www.orientdb.com
 */
package com.orientechnologies.spatial.shape;

import org.locationtech.jts.geom.Geometry;
import org.locationtech.spatial4j.shape.Shape;

/**
 * Created by Enrico Risa on 29/09/15.
 */
public class OShapeOperationImpl implements OShapeOperation {

  private final OShapeFactory factory;

  public OShapeOperationImpl(OShapeFactory oShapeFactory) {
    this.factory = oShapeFactory;
  }

  @Override
  public double distance(Shape s1, Shape s2) {
    Geometry geometry = factory.toGeometry(s1);
    Geometry geometry1 = factory.toGeometry(s2);
    return geometry.distance(geometry1);
  }

  @Override
  public boolean isWithInDistance(Shape s1, Shape s2, Double dist) {
    Geometry geometry = factory.toGeometry(s1);
    Geometry geometry1 = factory.toGeometry(s2);
    return geometry.isWithinDistance(geometry1, dist);
  }

  @Override
  public boolean intersect(Shape s1, Shape s2) {
    Geometry geometry = factory.toGeometry(s1);
    Geometry geometry1 = factory.toGeometry(s2);
    return geometry.intersects(geometry1);
  }
}
