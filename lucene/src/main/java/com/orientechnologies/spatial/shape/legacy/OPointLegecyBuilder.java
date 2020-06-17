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
package com.orientechnologies.spatial.shape.legacy;

import com.orientechnologies.orient.core.index.OCompositeKey;
import com.orientechnologies.orient.core.metadata.schema.OType;
import org.locationtech.spatial4j.context.SpatialContext;
import org.locationtech.spatial4j.shape.Point;

/** Created by Enrico Risa on 23/10/15. */
public class OPointLegecyBuilder implements OShapeBuilderLegacy<Point> {

  @Override
  public Point makeShape(OCompositeKey key, SpatialContext ctx) {
    double lat =
        ((Double) OType.convert(((OCompositeKey) key).getKeys().get(0), Double.class))
            .doubleValue();
    double lng =
        ((Double) OType.convert(((OCompositeKey) key).getKeys().get(1), Double.class))
            .doubleValue();
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
