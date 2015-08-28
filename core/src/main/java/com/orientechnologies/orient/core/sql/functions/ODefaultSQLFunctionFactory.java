/*
 * Copyright 2012 Orient Technologies.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.orientechnologies.orient.core.sql.functions;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.sql.functions.coll.*;
import com.orientechnologies.orient.core.sql.functions.geo.OSQLFunctionDistance;
import com.orientechnologies.orient.core.sql.functions.math.OSQLFunctionAverage;
import com.orientechnologies.orient.core.sql.functions.math.OSQLFunctionDecimal;
import com.orientechnologies.orient.core.sql.functions.math.OSQLFunctionEval;
import com.orientechnologies.orient.core.sql.functions.math.OSQLFunctionMax;
import com.orientechnologies.orient.core.sql.functions.math.OSQLFunctionMin;
import com.orientechnologies.orient.core.sql.functions.math.OSQLFunctionSum;
import com.orientechnologies.orient.core.sql.functions.misc.OSQLFunctionCoalesce;
import com.orientechnologies.orient.core.sql.functions.misc.OSQLFunctionCount;
import com.orientechnologies.orient.core.sql.functions.misc.OSQLFunctionDate;
import com.orientechnologies.orient.core.sql.functions.misc.OSQLFunctionDecode;
import com.orientechnologies.orient.core.sql.functions.misc.OSQLFunctionEncode;
import com.orientechnologies.orient.core.sql.functions.misc.OSQLFunctionIf;
import com.orientechnologies.orient.core.sql.functions.misc.OSQLFunctionIfNull;
import com.orientechnologies.orient.core.sql.functions.misc.OSQLFunctionSysdate;
import com.orientechnologies.orient.core.sql.functions.misc.OSQLFunctionUUID;
import com.orientechnologies.orient.core.sql.functions.stat.OSQLFunctionMedian;
import com.orientechnologies.orient.core.sql.functions.stat.OSQLFunctionMode;
import com.orientechnologies.orient.core.sql.functions.stat.OSQLFunctionPercentile;
import com.orientechnologies.orient.core.sql.functions.stat.OSQLFunctionStandardDeviation;
import com.orientechnologies.orient.core.sql.functions.stat.OSQLFunctionVariance;
import com.orientechnologies.orient.core.sql.functions.text.OSQLFunctionConcat;
import com.orientechnologies.orient.core.sql.functions.text.OSQLFunctionFormat;

/**
 * Default set of SQL function.
 * 
 * @author Johann Sorel (Geomatys)
 */
public final class ODefaultSQLFunctionFactory implements OSQLFunctionFactory {

  private static final Map<String, Object> FUNCTIONS = new HashMap<String, Object>();
  static {
    // MISC FUNCTIONS
    register(OSQLFunctionAverage.NAME, OSQLFunctionAverage.class);
    register(OSQLFunctionCoalesce.NAME, new OSQLFunctionCoalesce());
    register(OSQLFunctionCount.NAME, OSQLFunctionCount.class);
    register(OSQLFunctionDate.NAME, OSQLFunctionDate.class);
    register(OSQLFunctionDecode.NAME, new OSQLFunctionDecode());
    register(OSQLFunctionDifference.NAME, OSQLFunctionDifference.class);
    register(OSQLFunctionSymmetricDifference.NAME, OSQLFunctionSymmetricDifference.class);
    register(OSQLFunctionDistance.NAME, new OSQLFunctionDistance());
    register(OSQLFunctionDistinct.NAME, OSQLFunctionDistinct.class);
    register(OSQLFunctionDocument.NAME, OSQLFunctionDocument.class);
    register(OSQLFunctionEncode.NAME, new OSQLFunctionEncode());
    register(OSQLFunctionEval.NAME, OSQLFunctionEval.class);
    register(OSQLFunctionFirst.NAME, new OSQLFunctionFirst());
    register(OSQLFunctionFormat.NAME, new OSQLFunctionFormat());
    register(OSQLFunctionTraversedEdge.NAME, OSQLFunctionTraversedEdge.class);
    register(OSQLFunctionTraversedElement.NAME, OSQLFunctionTraversedElement.class);
    register(OSQLFunctionTraversedVertex.NAME, OSQLFunctionTraversedVertex.class);
    register(OSQLFunctionIf.NAME, new OSQLFunctionIf());
    register(OSQLFunctionIfNull.NAME, new OSQLFunctionIfNull());
    register(OSQLFunctionIntersect.NAME, OSQLFunctionIntersect.class);
    register(OSQLFunctionLast.NAME, new OSQLFunctionLast());
    register(OSQLFunctionList.NAME, OSQLFunctionList.class);
    register(OSQLFunctionMap.NAME, OSQLFunctionMap.class);
    register(OSQLFunctionMax.NAME, OSQLFunctionMax.class);
    register(OSQLFunctionMin.NAME, OSQLFunctionMin.class);
    register(OSQLFunctionSet.NAME, OSQLFunctionSet.class);
    register(OSQLFunctionSysdate.NAME, OSQLFunctionSysdate.class);
    register(OSQLFunctionSum.NAME, OSQLFunctionSum.class);
    register(OSQLFunctionUnionAll.NAME, OSQLFunctionUnionAll.class);
    register(OSQLFunctionMode.NAME, OSQLFunctionMode.class);
    register(OSQLFunctionPercentile.NAME, OSQLFunctionPercentile.class);
    register(OSQLFunctionMedian.NAME, OSQLFunctionMedian.class);
    register(OSQLFunctionVariance.NAME, OSQLFunctionVariance.class);
    register(OSQLFunctionStandardDeviation.NAME, OSQLFunctionStandardDeviation.class);
    register(OSQLFunctionUUID.NAME, OSQLFunctionUUID.class);
    register(OSQLFunctionConcat.NAME, OSQLFunctionConcat.class);
    register(OSQLFunctionDecimal.NAME, OSQLFunctionDecimal.class);
  }

  public static void register(final String iName, final Object iImplementation) {
    FUNCTIONS.put(iName.toLowerCase(), iImplementation);
  }

  @Override
  public Set<String> getFunctionNames() {
    return FUNCTIONS.keySet();
  }

  @Override
  public boolean hasFunction(final String name) {
    return FUNCTIONS.containsKey(name);
  }

  @Override
  public OSQLFunction createFunction(final String name) {
    final Object obj = FUNCTIONS.get(name);

    if (obj == null)
      throw new OCommandExecutionException("Unknown function name :" + name);

    if (obj instanceof OSQLFunction)
      return (OSQLFunction) obj;
    else {
      // it's a class
      final Class<?> clazz = (Class<?>) obj;
      try {
        return (OSQLFunction) clazz.newInstance();
      } catch (Exception e) {
        throw new OCommandExecutionException("Error in creation of function " + name
            + "(). Probably there is not an empty constructor or the constructor generates errors", e);
      }
    }

  }

}
