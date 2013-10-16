/*
 *
 * Copyright 2012 Luca Molino (molino.luca--AT--gmail.com)
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
package com.orientechnologies.orient.object.enhancement;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.lang.reflect.Method;

import javassist.util.proxy.MethodFilter;

import com.orientechnologies.common.log.OLogManager;

/**
 * @author luca.molino Original implementation
 * @author Janos Haber Scala binding
 * 
 */
public class OObjectMethodFilter implements MethodFilter {
  public boolean isHandled(final Method m) {
    final String methodName = m.getName();
    final String fieldName = getFieldName(m);

    if (fieldName == null)
      return false;

    try {
      if (!OObjectEntitySerializer.isClassField(m.getDeclaringClass(), fieldName))
        return false;
      return (isSetterMethod(methodName, m) || isGetterMethod(methodName, m));
    } catch (NoSuchFieldException nsfe) {
      OLogManager.instance().warn(this, "Error handling the method %s in class %s", nsfe, m.getName(),
          m.getDeclaringClass().getName());
      return false;
    } catch (SecurityException se) {
      OLogManager.instance().warn(this, "", se, m.getName(), m.getDeclaringClass().getName());
      return false;
    }
  }

  public String getFieldName(final Method m) {
    final String methodName = m.getName();
    final Class<?> clz = m.getDeclaringClass();

    if (methodName.startsWith("get"))
      return getFieldName(methodName, "get");
    else if (methodName.startsWith("set"))
      return getFieldName(methodName, "set");
    else if (methodName.startsWith("is"))
      return getFieldName(methodName, "is");
    else if (isScalaClass(clz)) {
      return getScalaFieldName(clz, methodName);
    }
    // NO FIELD
    return null;
  }

  protected String getFieldName(final String methodName, final String prefix) {
    final StringBuffer fieldName = new StringBuffer();
    fieldName.append(Character.toLowerCase(methodName.charAt(prefix.length())));
    fieldName.append(methodName.substring(prefix.length() + 1));
    return fieldName.toString();
  }

  public boolean isSetterMethod(final String methodName, final Method m) throws SecurityException, NoSuchFieldException {
    Class<?> clz = m.getDeclaringClass();
    if (!methodName.startsWith("set") || !checkIfFirstCharAfterPrefixIsUpperCase(methodName, "set")
        || (isScalaClass(clz) && !methodName.endsWith("_$eq")))
      return false;
    if (m.getParameterTypes() != null && m.getParameterTypes().length != 1)
      return false;
    if (OObjectEntitySerializer.isTransientField(m.getDeclaringClass(), getFieldName(m)))
      return false;
    Class<?>[] parameters = m.getParameterTypes();
    Field f = OObjectEntitySerializer.getField(getFieldName(m), m.getDeclaringClass());
    if (!f.getType().isAssignableFrom(parameters[0])) {
      OLogManager.instance().warn(
          this,
          "Setter method " + m.toString() + " for field " + f.getName() + " in class " + m.getDeclaringClass().toString()
              + " cannot be bound to proxied instance: parameter class don't match with field type " + f.getType().toString());
      return false;
    }
    return true;
  }

  public boolean isGetterMethod(String fieldName, Method m) throws SecurityException, NoSuchFieldException {
    int prefixLength;
    Class<?> clz = m.getDeclaringClass();
    if (fieldName.startsWith("get") && checkIfFirstCharAfterPrefixIsUpperCase(fieldName, "get"))
      prefixLength = "get".length();
    else if (fieldName.startsWith("is") && checkIfFirstCharAfterPrefixIsUpperCase(fieldName, "is"))
      prefixLength = "is".length();
    else if (isScalaClass(clz) && fieldName.equals(getFieldName(m)))
      prefixLength = 0;
    else
      return false;
    if (m.getParameterTypes() != null && m.getParameterTypes().length > 0)
      return false;
    if (fieldName.length() <= prefixLength)
      return false;
    return !OObjectEntitySerializer.isTransientField(m.getDeclaringClass(), getFieldName(m));
  }

  private boolean checkIfFirstCharAfterPrefixIsUpperCase(String methodName, String prefix) {
    return methodName.length() > prefix.length() ? Character.isUpperCase(methodName.charAt(prefix.length())) : false;
  }

  protected boolean isScalaClass(Class<?> clz) {
    Annotation[] annotations = clz.getDeclaredAnnotations();
    for (Annotation a : annotations) {
      if ("scala.reflect.ScalaSignature".contains(a.annotationType().getName())
          || "scala.reflect.ScalaLongSignature".contains(a.getClass().getName())) {
        return true;
      }
    }
    return false;
  }

  protected String getScalaFieldName(Class<?> clz, String name) {
    Field[] fields = clz.getDeclaredFields();
    for (Field field : fields) {
      if (name.equals(field.getName() + "_$eq")) {
        return field.getName();
      } else if (name.equals(field.getName())) {
        return field.getName();
      }
    }
    return null;
  }
}
