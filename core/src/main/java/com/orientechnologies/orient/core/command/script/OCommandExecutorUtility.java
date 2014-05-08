/*
 * Copyright 2010-2012 Luca Garulli (l.garulli--at--orientechnologies.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.orientechnologies.orient.core.command.script;

import java.lang.reflect.Method;
import java.util.Map;

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
  public static Object transformResult(final Object result) {
    // PATCH BY MAT ABOUT NASHORN RETURNING VALUE FOR ARRAYS. TEST IF 0 IS PRESENT AS KEY. IN THIS CASE RETURNS THE VALUES NOT THE
    // OBJECT AS MAP
    if (result instanceof Map)
      try {
        if (((Map) result).containsKey("0"))
          return ((Map) result).values();
      } catch (Exception e) {
      }

    return result;
  }
}
