/*
 *
 *  *  Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
 *  *
 *  *  Licensed under the Apache License, Version 2.0 (the "License");
 *  *  you may not use this file except in compliance with the License.
 *  *  You may obtain a copy of the License at
 *  *
 *  *       http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  *  Unless required by applicable law or agreed to in writing, software
 *  *  distributed under the License is distributed on an "AS IS" BASIS,
 *  *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  *  See the License for the specific language governing permissions and
 *  *  limitations under the License.
 *  *
 *  * For more information: http://orientdb.com
 *
 */
package com.orientechnologies.orient.core.sql.functions.geo;

import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.sql.functions.OSQLFunctionAbstract;

/**
 * Haversine formula to compute the distance between 2 gro points.
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 */
public class OSQLFunctionDistance extends OSQLFunctionAbstract {
  public static final String NAME = "distance";

  private static final double EARTH_RADIUS = 6371;

  public OSQLFunctionDistance() {
    super(NAME, 4, 5);
  }

  public Object execute(
      Object iThis,
      final OIdentifiable iCurrentRecord,
      Object iCurrentResult,
      final Object[] iParams,
      OCommandContext iContext) {
    double distance;

    final double[] values = new double[4];

    for (int i = 0; i < iParams.length && i < 4; ++i) {
      if (iParams[i] == null) return null;

      values[i] = ((Double) OType.convert(iParams[i], Double.class)).doubleValue();
    }

    final double deltaLat = Math.toRadians(values[2] - values[0]);
    final double deltaLon = Math.toRadians(values[3] - values[1]);

    final double a =
        Math.pow(Math.sin(deltaLat / 2), 2)
            + Math.cos(Math.toRadians(values[0]))
                * Math.cos(Math.toRadians(values[2]))
                * Math.pow(Math.sin(deltaLon / 2), 2);
    distance = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a)) * EARTH_RADIUS;

    if (iParams.length > 4) {
      final String unit = iParams[4].toString();
      if (unit.equalsIgnoreCase("km"))
        // ALREADY IN KM
        ;
      else if (unit.equalsIgnoreCase("mi"))
        // MILES
        distance *= 0.621371192;
      else if (unit.equalsIgnoreCase("nmi"))
        // NAUTICAL MILES
        distance *= 0.539956803;
      else
        throw new IllegalArgumentException(
            "Unsupported unit '" + unit + "'. Use km, mi and nmi. Default is km.");
    }

    return distance;
  }

  public String getSyntax() {
    return "distance(<field-x>,<field-y>,<x-value>,<y-value>[,<unit>])";
  }
}
