/*
  *
  *  *  Copyright 2014 Orient Technologies LTD (info(at)orientechnologies.com)
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
  *  * For more information: http://www.orientechnologies.com
  *
  */
package com.orientechnologies.orient.core.command.script;

import java.lang.reflect.Method;
import java.util.*;

/**
 * Script utility class
 * 
 * @see OCommandScript
 * @author Luca Garulli
 * 
 */
public class OCommandExecutorUtility {
  private static Method java8MethodIsArray;
  private static Method java8MethodValues;

  /**
   * Manages cross compiler compatibility issues.
   * 
   * @param result
   *          Result to transform
   * @return
   */
  public static Object transformResult(Object result) {
    // PATCH BY MAT ABOUT NASHORN RETURNING VALUE FOR ARRAYS.
    if (result!=null && result.getClass().getName().contains("nashorn") && result instanceof Map) {
      try {
        if (isActuallyAnArray(result)) {
          List<?> partial = new ArrayList(((Map) result).values());
          List<Object> finalResult = new ArrayList<Object>();
          for (Object o : partial) {
            if (o instanceof Map) {
              Map<Object, Object> innerMap = (Map) o;
              Set<Object> keys = innerMap.keySet();
              Map<Object, Object> newMap = new LinkedHashMap<Object, Object>();
              for (Object key : keys) {
                newMap.put(key, transformResult(innerMap.get(key)));
              }
              finalResult.add(newMap.values());
            } else {
              finalResult.add(o);
            }
          }

          return finalResult;
        }else{
          Map<Object, Object> mapResult = (Map) result;
          List<Object> keys = new ArrayList<Object>(mapResult.keySet());
          for (Object key : keys) {
            mapResult.put(key, transformResult(mapResult.get(key)));
          }
          return mapResult;
        }
      } catch (Exception e) {
      }
    }

    return result;
  }

  private static boolean isActuallyAnArray(Object iObject) {
    try {
      return Boolean.TRUE.equals(Class.forName("jdk.nashorn.api.scripting.JSObject").getDeclaredMethod("isArray", null).invoke(iObject));
    }  catch(Exception e) {
      return false;
    }
  }
}
