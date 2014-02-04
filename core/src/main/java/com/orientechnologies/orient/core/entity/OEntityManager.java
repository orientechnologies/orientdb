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
package com.orientechnologies.orient.core.entity;

import java.lang.reflect.InvocationTargetException;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.reflection.OReflectionHelper;
import com.orientechnologies.orient.core.exception.OConfigurationException;
import com.orientechnologies.orient.core.metadata.security.ORole;
import com.orientechnologies.orient.core.metadata.security.OUser;

public class OEntityManager {
  private static Map<String, OEntityManager> databaseInstances = new HashMap<String, OEntityManager>();
  private OEntityManagerClassHandler         classHandler      = new OEntityManagerClassHandler();

  protected OEntityManager() {
    OLogManager.instance().debug(this, "Registering entity manager");

    classHandler.registerEntityClass(OUser.class);
    classHandler.registerEntityClass(ORole.class);
  }

  public static synchronized OEntityManager getEntityManagerByDatabaseURL(final String iURL) {
    OEntityManager instance = databaseInstances.get(iURL);
    if (instance == null) {
      instance = new OEntityManager();
      databaseInstances.put(iURL, instance);
    }
    return instance;
  }

  /**
   * Create a POJO by its class name.
   * 
   * @see #registerEntityClasses(String)
   */
  public synchronized Object createPojo(final String iClassName) throws OConfigurationException {
    if (iClassName == null)
      throw new IllegalArgumentException("Cannot create the object: class name is empty");

    final Class<?> entityClass = classHandler.getEntityClass(iClassName);

    try {
      if (entityClass != null)
        return createInstance(entityClass);

    } catch (Exception e) {
      throw new OConfigurationException("Error while creating new pojo of class '" + iClassName + "'", e);
    }

    try {
      // TRY TO INSTANTIATE THE CLASS DIRECTLY BY ITS NAME
      return createInstance(Class.forName(iClassName));
    } catch (Exception e) {
      throw new OConfigurationException("The class '" + iClassName
          + "' was not found between the entity classes. Ensure registerEntityClasses(package) has been called first.", e);
    }
  }

  /**
   * Returns the Java class by its name
   * 
   * @param iClassName
   *          Simple class name without the package
   * @return Returns the Java class by its name
   */
  public synchronized Class<?> getEntityClass(final String iClassName) {
    return classHandler.getEntityClass(iClassName);
  }

  public synchronized void deregisterEntityClass(final Class<?> iClass) {
    classHandler.deregisterEntityClass(iClass);
  }

  public synchronized void deregisterEntityClasses(final String iPackageName) {
    deregisterEntityClasses(iPackageName, Thread.currentThread().getContextClassLoader());
  }

  /**
   * Scans all classes accessible from the context class loader which belong to the given package and subpackages.
   * 
   * @param iPackageName
   *          The base package
   */
  public synchronized void deregisterEntityClasses(final String iPackageName, final ClassLoader iClassLoader) {
    OLogManager.instance().debug(this, "Discovering entity classes inside package: %s", iPackageName);

    List<Class<?>> classes = null;
    try {
      classes = OReflectionHelper.getClassesFor(iPackageName, iClassLoader);
    } catch (ClassNotFoundException e) {
      throw new OException(e);
    }
    for (Class<?> c : classes) {
      deregisterEntityClass(c);
    }

    if (OLogManager.instance().isDebugEnabled()) {
      for (Entry<String, Class<?>> entry : classHandler.getClassesEntrySet()) {
        OLogManager.instance().debug(this, "Unloaded entity class '%s' from: %s", entry.getKey(), entry.getValue());
      }
    }
  }

  public synchronized void registerEntityClass(final Class<?> iClass) {
    classHandler.registerEntityClass(iClass);
  }

  /**
   * Registers provided classes
   * 
   * @param iClassNames
   *          to be registered
   */
  public synchronized void registerEntityClasses(final Collection<String> iClassNames) {
    registerEntityClasses(iClassNames, Thread.currentThread().getContextClassLoader());
  }

  /**
   * Registers provided classes
   * 
   * @param iClassNames
   *          to be registered
   * @param iClassLoader
   */
  public synchronized void registerEntityClasses(final Collection<String> iClassNames, final ClassLoader iClassLoader) {
    OLogManager.instance().debug(this, "Discovering entity classes for class names: %s", iClassNames);

    try {
      registerEntityClasses(OReflectionHelper.getClassesFor(iClassNames, iClassLoader));
    } catch (ClassNotFoundException e) {
      throw new OException(e);
    }
  }

  /**
   * Scans all classes accessible from the context class loader which belong to the given package and subpackages.
   * 
   * @param iPackageName
   *          The base package
   */
  public synchronized void registerEntityClasses(final String iPackageName) {
    registerEntityClasses(iPackageName, Thread.currentThread().getContextClassLoader());
  }

  /**
   * Scans all classes accessible from the context class loader which belong to the given package and subpackages.
   * 
   * @param iPackageName
   *          The base package
   * @param iClassLoader
   */
  public synchronized void registerEntityClasses(final String iPackageName, final ClassLoader iClassLoader) {
    OLogManager.instance().debug(this, "Discovering entity classes inside package: %s", iPackageName);

    try {
      registerEntityClasses(OReflectionHelper.getClassesFor(iPackageName, iClassLoader));
    } catch (ClassNotFoundException e) {
      throw new OException(e);
    }
  }

  protected synchronized void registerEntityClasses(final List<Class<?>> classes) {
    for (Class<?> c : classes) {
      if (!classHandler.containsEntityClass(c)) {
        if (c.isAnonymousClass()) {
          OLogManager.instance().debug(this, "Skip registration of anonymous class '%s'.", c.getName());
          continue;
        }
        classHandler.registerEntityClass(c);
      }
    }

    if (OLogManager.instance().isDebugEnabled()) {
      for (Entry<String, Class<?>> entry : classHandler.getClassesEntrySet()) {
        OLogManager.instance().debug(this, "Loaded entity class '%s' from: %s", entry.getKey(), entry.getValue());
      }
    }
  }

  /**
   * Sets the received handler as default and merges the classes all together.
   * 
   * @param iClassHandler
   */
  public synchronized void setClassHandler(final OEntityManagerClassHandler iClassHandler) {
    for (Entry<String, Class<?>> entry : classHandler.getClassesEntrySet()) {
      iClassHandler.registerEntityClass(entry.getValue());
    }
    this.classHandler = iClassHandler;
  }

  public synchronized Collection<Class<?>> getRegisteredEntities() {
    return classHandler.getRegisteredEntities();
  }

  protected Object createInstance(final Class<?> iClass) throws InstantiationException, IllegalAccessException,
      InvocationTargetException {
    return classHandler.createInstance(iClass);
  }
}
