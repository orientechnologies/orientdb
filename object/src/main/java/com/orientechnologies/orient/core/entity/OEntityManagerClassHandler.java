/*
 *
 * Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
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
package com.orientechnologies.orient.core.entity;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

/** @author Luca Molino (molino.luca--at--gmail.com) */
public class OEntityManagerClassHandler {

  private Map<String, Class<?>> entityClasses = new HashMap<String, Class<?>>();

  /**
   * Returns the Java class by its name
   *
   * @param iClassName Simple class name without the package
   * @return Returns the Java class by its name
   */
  public synchronized Class<?> getEntityClass(final String iClassName) {
    return entityClasses.get(iClassName);
  }

  public synchronized void registerEntityClass(final Class<?> iClass) {
    entityClasses.put(iClass.getSimpleName(), iClass);
  }

  public synchronized void registerEntityClass(final Class<?> iClass, boolean forceSchemaReload) {
    entityClasses.put(iClass.getSimpleName(), iClass);
  }

  public synchronized void registerEntityClass(final String iClassName, final Class<?> iClass) {
    entityClasses.put(iClassName, iClass);
  }

  public synchronized void registerEntityClass(
      final String iClassName, final Class<?> iClass, boolean forceSchemaReload) {
    entityClasses.put(iClassName, iClass);
  }

  public synchronized void deregisterEntityClass(final String iClassName) {
    entityClasses.remove(iClassName);
  }

  public synchronized void deregisterEntityClass(final Class<?> iClass) {
    entityClasses.remove(iClass.getSimpleName());
  }

  public synchronized Set<Entry<String, Class<?>>> getClassesEntrySet() {
    return entityClasses.entrySet();
  }

  public synchronized boolean containsEntityClass(final String iClassName) {
    return entityClasses.containsKey(iClassName);
  }

  public synchronized boolean containsEntityClass(final Class<?> iClass) {
    return entityClasses.containsKey(iClass.getSimpleName());
  }

  public synchronized Object createInstance(final Class<?> iClass)
      throws InstantiationException, IllegalAccessException, InvocationTargetException {
    Constructor<?> defaultConstructor = null;
    for (Constructor<?> c : iClass.getConstructors()) {
      if (c.getParameterTypes().length == 0) {
        defaultConstructor = c;
        break;
      }
    }

    if (defaultConstructor == null)
      throw new IllegalArgumentException(
          "Cannot create an object of class '"
              + iClass.getName()
              + "' because it has no default constructor. Please define the method: "
              + iClass.getSimpleName()
              + "()");

    if (!defaultConstructor.isAccessible())
      // OVERRIDE PROTECTION
      defaultConstructor.setAccessible(true);

    return defaultConstructor.newInstance();
  }

  public synchronized Collection<Class<?>> getRegisteredEntities() {
    return entityClasses.values();
  }
}
