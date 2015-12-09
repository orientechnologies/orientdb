/*
 *
 *  * Copyright 2014 Orient Technologies.
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  *      http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */
package com.orientechnologies.spatial.functions;

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.sql.functions.OSQLFunction;
import com.orientechnologies.orient.core.sql.functions.OSQLFunctionFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class OSpatialFunctionsFactory implements OSQLFunctionFactory {

  public static final Map<String, Object> FUNCTIONS = new HashMap<String, Object>();

  static {
    register(OSTGeomFromTextFunction.NAME, new OSTGeomFromTextFunction());
    register(OSTAsTextFunction.NAME, new OSTAsTextFunction());
    register(OSTWithinFunction.NAME, new OSTWithinFunction());
    register(OSTDWithinFunction.NAME, new OSTDWithinFunction());
    register(OSTEqualsFunction.NAME, new OSTEqualsFunction());
    register(OSTAsBinaryFunction.NAME, new OSTAsBinaryFunction());
    register(OSTEnvelopFunction.NAME, new OSTEnvelopFunction());
    register(OSTBufferFunction.NAME, new OSTBufferFunction());
    register(OSTDistanceFunction.NAME, new OSTDistanceFunction());
    register(OSTDisjointFunction.NAME, new OSTDisjointFunction());
    register(OSTIntersectsFunction.NAME, new OSTIntersectsFunction());
    register(OSTContainsFunction.NAME, new OSTContainsFunction());
    register(OSTSrid.NAME, new OSTSrid());
  }

  public static void register(final String iName, final Object iImplementation) {
    FUNCTIONS.put(iName.toLowerCase(), iImplementation);
  }

  @Override
  public boolean hasFunction(String iName) {
    return FUNCTIONS.containsKey(iName);
  }

  @Override
  public Set<String> getFunctionNames() {
    return FUNCTIONS.keySet();
  }

  @Override
  public OSQLFunction createFunction(String name) throws OCommandExecutionException {
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
        throw OException.wrapException(new OCommandExecutionException("Error in creation of function " + name
            + "(). Probably there is not an empty constructor or the constructor generates errors"), e);
      }
    }

  }

  public Map<String, Object> getFunctions() {
    return FUNCTIONS;
  }
}
