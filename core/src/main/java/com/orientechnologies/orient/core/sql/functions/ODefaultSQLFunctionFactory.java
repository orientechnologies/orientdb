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
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.sql.functions.coll.OSQLFunctionDifference;
import com.orientechnologies.orient.core.sql.functions.coll.OSQLFunctionDistinct;
import com.orientechnologies.orient.core.sql.functions.coll.OSQLFunctionDocument;
import com.orientechnologies.orient.core.sql.functions.coll.OSQLFunctionFirst;
import com.orientechnologies.orient.core.sql.functions.coll.OSQLFunctionIntersect;
import com.orientechnologies.orient.core.sql.functions.coll.OSQLFunctionLast;
import com.orientechnologies.orient.core.sql.functions.coll.OSQLFunctionList;
import com.orientechnologies.orient.core.sql.functions.coll.OSQLFunctionMap;
import com.orientechnologies.orient.core.sql.functions.coll.OSQLFunctionSet;
import com.orientechnologies.orient.core.sql.functions.coll.OSQLFunctionUnion;
import com.orientechnologies.orient.core.sql.functions.geo.OSQLFunctionDistance;
import com.orientechnologies.orient.core.sql.functions.math.OSQLFunctionAverage;
import com.orientechnologies.orient.core.sql.functions.math.OSQLFunctionEval;
import com.orientechnologies.orient.core.sql.functions.math.OSQLFunctionMax;
import com.orientechnologies.orient.core.sql.functions.math.OSQLFunctionMin;
import com.orientechnologies.orient.core.sql.functions.math.OSQLFunctionSum;
import com.orientechnologies.orient.core.sql.functions.misc.OSQLFunctionCoalesce;
import com.orientechnologies.orient.core.sql.functions.misc.OSQLFunctionCount;
import com.orientechnologies.orient.core.sql.functions.misc.OSQLFunctionDate;
import com.orientechnologies.orient.core.sql.functions.misc.OSQLFunctionDecode;
import com.orientechnologies.orient.core.sql.functions.misc.OSQLFunctionEncode;
import com.orientechnologies.orient.core.sql.functions.misc.OSQLFunctionFormat;
import com.orientechnologies.orient.core.sql.functions.misc.OSQLFunctionIf;
import com.orientechnologies.orient.core.sql.functions.misc.OSQLFunctionIfNull;
import com.orientechnologies.orient.core.sql.functions.misc.OSQLFunctionSysdate;

/**
 * Default set of SQL functions.
 * 
 * @author Johann Sorel (Geomatys)
 */
public final class ODefaultSQLFunctionFactory implements OSQLFunctionFactory {

  private static final Map<String, Object> FUNCTIONS = new HashMap<String, Object>();
  static {
    // MISC FUNCTIONS
    FUNCTIONS.put(OSQLFunctionCoalesce.NAME.toUpperCase(Locale.ENGLISH), new OSQLFunctionCoalesce());
    FUNCTIONS.put(OSQLFunctionIf.NAME.toUpperCase(Locale.ENGLISH), new OSQLFunctionIf());
    FUNCTIONS.put(OSQLFunctionIfNull.NAME.toUpperCase(Locale.ENGLISH), new OSQLFunctionIfNull());
    FUNCTIONS.put(OSQLFunctionFormat.NAME.toUpperCase(Locale.ENGLISH), new OSQLFunctionFormat());
    FUNCTIONS.put(OSQLFunctionDate.NAME.toUpperCase(Locale.ENGLISH), OSQLFunctionDate.class);
    FUNCTIONS.put(OSQLFunctionSysdate.NAME.toUpperCase(Locale.ENGLISH), OSQLFunctionSysdate.class);
    FUNCTIONS.put(OSQLFunctionCount.NAME.toUpperCase(Locale.ENGLISH), OSQLFunctionCount.class);
    FUNCTIONS.put(OSQLFunctionDocument.NAME.toUpperCase(Locale.ENGLISH), OSQLFunctionDocument.class);
    FUNCTIONS.put(OSQLFunctionDistinct.NAME.toUpperCase(Locale.ENGLISH), OSQLFunctionDistinct.class);
    FUNCTIONS.put(OSQLFunctionUnion.NAME.toUpperCase(Locale.ENGLISH), OSQLFunctionUnion.class);
    FUNCTIONS.put(OSQLFunctionIntersect.NAME.toUpperCase(Locale.ENGLISH), OSQLFunctionIntersect.class);
    FUNCTIONS.put(OSQLFunctionDifference.NAME.toUpperCase(Locale.ENGLISH), OSQLFunctionDifference.class);
    FUNCTIONS.put(OSQLFunctionFirst.NAME.toUpperCase(Locale.ENGLISH), OSQLFunctionFirst.class);
    FUNCTIONS.put(OSQLFunctionLast.NAME.toUpperCase(Locale.ENGLISH), OSQLFunctionLast.class);
    FUNCTIONS.put(OSQLFunctionList.NAME.toUpperCase(Locale.ENGLISH), OSQLFunctionList.class);
    FUNCTIONS.put(OSQLFunctionSet.NAME.toUpperCase(Locale.ENGLISH), OSQLFunctionSet.class);
    FUNCTIONS.put(OSQLFunctionMap.NAME.toUpperCase(Locale.ENGLISH), OSQLFunctionMap.class);
    FUNCTIONS.put(OSQLFunctionEncode.NAME.toUpperCase(Locale.ENGLISH), new OSQLFunctionEncode());
    FUNCTIONS.put(OSQLFunctionDecode.NAME.toUpperCase(Locale.ENGLISH), new OSQLFunctionDecode());

    // MATH FUNCTIONS
    FUNCTIONS.put(OSQLFunctionMin.NAME.toUpperCase(Locale.ENGLISH), OSQLFunctionMin.class);
    FUNCTIONS.put(OSQLFunctionMax.NAME.toUpperCase(Locale.ENGLISH), OSQLFunctionMax.class);
    FUNCTIONS.put(OSQLFunctionSum.NAME.toUpperCase(Locale.ENGLISH), OSQLFunctionSum.class);
    FUNCTIONS.put(OSQLFunctionAverage.NAME.toUpperCase(Locale.ENGLISH), OSQLFunctionAverage.class);
    FUNCTIONS.put(OSQLFunctionEval.NAME.toUpperCase(Locale.ENGLISH), OSQLFunctionEval.class);

    // GEO FUNCTIONS
    FUNCTIONS.put(OSQLFunctionDistance.NAME.toUpperCase(Locale.ENGLISH), new OSQLFunctionDistance());
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
      throw new OCommandExecutionException("Unknowned function name :" + name);

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
