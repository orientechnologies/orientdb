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
package com.orientechnologies.common.parser;

import java.lang.reflect.Field;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Resolve system variables embedded in a String.
 * 
 * @author Luca Garulli (luca.garulli--at--assetdata.it)
 * 
 */
public class OSystemVariableResolver implements OVariableParserListener {
  public static final String             VAR_BEGIN = "${";
  public static final String             VAR_END   = "}";

  private static OSystemVariableResolver instance  = new OSystemVariableResolver();

  public static String resolveSystemVariables(final String iPath) {
    return resolveSystemVariables(iPath, null);
  }

  public static String resolveSystemVariables(final String iPath, final String iDefault) {
    if (iPath == null)
      return iDefault;

    return (String) OVariableParser.resolveVariables(iPath, VAR_BEGIN, VAR_END, instance, iDefault);
  }

  public static String resolveVariable(final String variable) {
    if (variable == null)
      return null;

    String resolved = System.getProperty(variable);

    if (resolved == null)
      // TRY TO FIND THE VARIABLE BETWEEN SYSTEM'S ENVIRONMENT PROPERTIES
      resolved = System.getenv(variable);

    return resolved;
  }

  @Override
  public String resolve(final String variable) {
    return resolveVariable(variable);
  }

  public static void setEnv(final String name, String value) {
    final Map<String, String> map = new HashMap<String, String>(System.getenv());
    map.put(name, value);
    setEnv(map);
  }

  public static void setEnv(final Map<String, String> newenv) {
    try {
      Class<?> processEnvironmentClass = Class.forName("java.lang.ProcessEnvironment");
      Field theEnvironmentField = processEnvironmentClass.getDeclaredField("theEnvironment");
      theEnvironmentField.setAccessible(true);
      Map<String, String> env = (Map<String, String>) theEnvironmentField.get(null);
      env.putAll(newenv);
      Field theCaseInsensitiveEnvironmentField = processEnvironmentClass.getDeclaredField("theCaseInsensitiveEnvironment");
      theCaseInsensitiveEnvironmentField.setAccessible(true);
      Map<String, String> cienv = (Map<String, String>) theCaseInsensitiveEnvironmentField.get(null);
      cienv.putAll(newenv);
    } catch (NoSuchFieldException e) {
      try {
        Class[] classes = Collections.class.getDeclaredClasses();
        Map<String, String> env = System.getenv();
        for (Class cl : classes) {
          if ("java.util.Collections$UnmodifiableMap".equals(cl.getName())) {
            Field field = cl.getDeclaredField("m");
            field.setAccessible(true);
            Object obj = field.get(env);
            Map<String, String> map = (Map<String, String>) obj;
            map.clear();
            map.putAll(newenv);
          }
        }
      } catch (Exception e2) {
        e2.printStackTrace();
      }
    } catch (Exception e1) {
      e1.printStackTrace();
    }
  }
}
