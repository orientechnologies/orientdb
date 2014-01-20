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
import com.orientechnologies.orient.core.sql.functions.coll.OSQLFunctionDifference;
import com.orientechnologies.orient.core.sql.functions.coll.OSQLFunctionDistinct;
import com.orientechnologies.orient.core.sql.functions.coll.OSQLFunctionDocument;
import com.orientechnologies.orient.core.sql.functions.coll.OSQLFunctionFirst;
import com.orientechnologies.orient.core.sql.functions.coll.OSQLFunctionIntersect;
import com.orientechnologies.orient.core.sql.functions.coll.OSQLFunctionLast;
import com.orientechnologies.orient.core.sql.functions.coll.OSQLFunctionList;
import com.orientechnologies.orient.core.sql.functions.coll.OSQLFunctionMap;
import com.orientechnologies.orient.core.sql.functions.coll.OSQLFunctionMultiValue;
import com.orientechnologies.orient.core.sql.functions.coll.OSQLFunctionSet;
import com.orientechnologies.orient.core.sql.functions.coll.OSQLFunctionUnion;
import com.orientechnologies.orient.core.sql.functions.conversion.OSQLFunctionAsDate;
import com.orientechnologies.orient.core.sql.functions.conversion.OSQLFunctionAsDateTime;
import com.orientechnologies.orient.core.sql.functions.conversion.OSQLFunctionAsDecimal;
import com.orientechnologies.orient.core.sql.functions.conversion.OSQLFunctionConvert;
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
import com.orientechnologies.orient.core.sql.functions.misc.OSQLFunctionExclude;
import com.orientechnologies.orient.core.sql.functions.misc.OSQLFunctionIf;
import com.orientechnologies.orient.core.sql.functions.misc.OSQLFunctionIfNull;
import com.orientechnologies.orient.core.sql.functions.misc.OSQLFunctionInclude;
import com.orientechnologies.orient.core.sql.functions.misc.OSQLFunctionSysdate;
import com.orientechnologies.orient.core.sql.functions.text.OSQLFunctionAppend;
import com.orientechnologies.orient.core.sql.functions.text.OSQLFunctionCharAt;
import com.orientechnologies.orient.core.sql.functions.text.OSQLFunctionFormat;
import com.orientechnologies.orient.core.sql.functions.text.OSQLFunctionLeft;
import com.orientechnologies.orient.core.sql.functions.text.OSQLFunctionLength;
import com.orientechnologies.orient.core.sql.functions.text.OSQLFunctionReplace;
import com.orientechnologies.orient.core.sql.functions.text.OSQLFunctionRight;
import com.orientechnologies.orient.core.sql.functions.text.OSQLFunctionSubString;
import com.orientechnologies.orient.core.sql.functions.text.OSQLFunctionToJSON;

/**
 * Default set of SQL functions.
 * 
 * @author Johann Sorel (Geomatys)
 */
public final class ODefaultSQLFunctionFactory implements OSQLFunctionFactory {

  private static final Map<String, Object> FUNCTIONS = new HashMap<String, Object>();
  static {
    // MISC FUNCTIONS
    register(OSQLFunctionAppend.NAME, new OSQLFunctionAppend());
    register(OSQLFunctionAsDate.NAME, new OSQLFunctionAsDate());
    register(OSQLFunctionAsDateTime.NAME, new OSQLFunctionAsDateTime());
    register(OSQLFunctionAsDecimal.NAME, new OSQLFunctionAsDecimal());
    register(OSQLFunctionAverage.NAME, OSQLFunctionAverage.class);
    register(OSQLFunctionCharAt.NAME, new OSQLFunctionCharAt());
    register(OSQLFunctionCoalesce.NAME, new OSQLFunctionCoalesce());
    register(OSQLFunctionConvert.NAME, new OSQLFunctionConvert());
    register(OSQLFunctionCount.NAME, OSQLFunctionCount.class);
    register(OSQLFunctionDate.NAME, OSQLFunctionDate.class);
    register(OSQLFunctionDecode.NAME, new OSQLFunctionDecode());
    register(OSQLFunctionDifference.NAME, OSQLFunctionDifference.class);
    register(OSQLFunctionDistance.NAME, new OSQLFunctionDistance());
    register(OSQLFunctionDistinct.NAME, OSQLFunctionDistinct.class);
    register(OSQLFunctionDocument.NAME, OSQLFunctionDocument.class);
    register(OSQLFunctionEncode.NAME, new OSQLFunctionEncode());
    register(OSQLFunctionExclude.NAME, new OSQLFunctionExclude());
    register(OSQLFunctionEval.NAME, OSQLFunctionEval.class);
    register(OSQLFunctionFirst.NAME, OSQLFunctionFirst.class);
    register(OSQLFunctionFormat.NAME, new OSQLFunctionFormat());
    register(OSQLFunctionIf.NAME, new OSQLFunctionIf());
    register(OSQLFunctionIfNull.NAME, new OSQLFunctionIfNull());
    register(OSQLFunctionInclude.NAME, new OSQLFunctionInclude());
    register(OSQLFunctionIntersect.NAME, OSQLFunctionIntersect.class);
    register(OSQLFunctionLast.NAME, OSQLFunctionLast.class);
    register(OSQLFunctionLeft.NAME, new OSQLFunctionLeft());
    register(OSQLFunctionLength.NAME, new OSQLFunctionLength());
    register(OSQLFunctionList.NAME, OSQLFunctionList.class);
    register(OSQLFunctionMap.NAME, OSQLFunctionMap.class);
    register(OSQLFunctionMax.NAME, OSQLFunctionMax.class);
    register(OSQLFunctionMin.NAME, OSQLFunctionMin.class);
    register(OSQLFunctionMultiValue.NAME, new OSQLFunctionMultiValue());
    register(OSQLFunctionReplace.NAME, new OSQLFunctionReplace());
    register(OSQLFunctionRight.NAME, new OSQLFunctionRight());
    register(OSQLFunctionSet.NAME, OSQLFunctionSet.class);
    register(OSQLFunctionSysdate.NAME, OSQLFunctionSysdate.class);
    register(OSQLFunctionSubString.NAME, new OSQLFunctionSubString());
    register(OSQLFunctionSum.NAME, OSQLFunctionSum.class);
    register(OSQLFunctionToJSON.NAME, new OSQLFunctionToJSON());
    register(OSQLFunctionUnion.NAME, OSQLFunctionUnion.class);
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
