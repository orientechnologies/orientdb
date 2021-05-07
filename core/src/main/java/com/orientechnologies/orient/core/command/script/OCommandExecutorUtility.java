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
package com.orientechnologies.orient.core.command.script;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Script utility class
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 * @see OCommandScript
 */
public class OCommandExecutorUtility {
  private static final Method java8MethodIsArray;

  static {
    Method isArray = null;

    if (!OGlobalConfiguration.SCRIPT_POLYGLOT_USE_GRAAL.getValueAsBoolean())
      try {
        isArray =
            Class.forName("jdk.nashorn.api.scripting.JSObject").getDeclaredMethod("isArray", null);
      } catch (LinkageError
          | ClassNotFoundException
          | NoSuchMethodException
          | SecurityException ignore) {
      }

    java8MethodIsArray = isArray;
  }

  /**
   * Manages cross compiler compatibility issues.
   *
   * @param result Result to transform
   */
  public static Object transformResult(Object result) {
    if (java8MethodIsArray == null || !(result instanceof Map)) {
      return result;
    }
    // PATCH BY MAT ABOUT NASHORN RETURNING VALUE FOR ARRAYS.
    try {
      if ((Boolean) java8MethodIsArray.invoke(result)) {
        List<?> partial = new ArrayList(((Map) result).values());
        List<Object> finalResult = new ArrayList<Object>();
        for (Object o : partial) {
          finalResult.add(transformResult(o));
        }
        return finalResult;
      } else {
        Map<Object, Object> mapResult = (Map) result;
        List<Object> keys = new ArrayList<Object>(mapResult.keySet());
        for (Object key : keys) {
          mapResult.put(key, transformResult(mapResult.get(key)));
        }
        return mapResult;
      }
    } catch (Exception e) {
      OLogManager.instance().error(OCommandExecutorUtility.class, "", e);
    }

    return result;
  }
}
