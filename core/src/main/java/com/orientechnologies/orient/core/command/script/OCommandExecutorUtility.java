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
   * Manage cross compiler compatibility issues.
   * 
   * @param ob
   * @return
   */
  public static Object transformResult(final Object ob) {
//    if( ob == null )
//      return ob
//
//    // JAVA8 NASHORN COMPATIBILITY OR RETURNING ARRAYS
//    if (java8MethodIsArray != null || ob.getClass().getName().equals("jdk.nashorn.api.scripting.ScriptObjectMirror")) {
//      try {
//        if (java8MethodValues == null) {
//          //java8MethodIsArray = ob.getClass().getMethod("isArray");
//          java8MethodValues = ob.getClass().getMethod("values");
//          java8MethodValues.setAccessible(true);
//        }
//
//        Method m = ob.getClass().getMethod("entrySet");
//        m.setAccessible(true);
//        Object r = m.invoke(ob);
//
//        //        final Boolean isArray = (Boolean) java8MethodIsArray.invoke(ob);
////
////        if (isArray)
//          return java8MethodValues.invoke(ob);
//
//      } catch (Exception e) {
//        OLogManager.instance().warn(OCommandExecutorUtility.class, "Error on conversion object from Nashorn engine", e);
//      }
//    }

    return ob;
  }
}
