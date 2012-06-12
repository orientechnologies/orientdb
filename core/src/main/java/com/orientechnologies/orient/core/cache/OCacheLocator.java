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
package com.orientechnologies.orient.core.cache;

import static com.orientechnologies.orient.core.config.OGlobalConfiguration.CACHE_LEVEL1_SIZE;
import static com.orientechnologies.orient.core.config.OGlobalConfiguration.CACHE_LEVEL2_IMPL;
import static com.orientechnologies.orient.core.config.OGlobalConfiguration.CACHE_LEVEL2_SIZE;

import java.lang.reflect.Constructor;

import com.orientechnologies.common.log.OLogManager;

/**
 * Creates primary and secondary caches using configuration to look up for cache implementations in classpath.
 */
public class OCacheLocator {
  public OCache primaryCache() {
    return new ODefaultCache(null, CACHE_LEVEL1_SIZE.getValueAsInteger());
  }

  public OCache secondaryCache(final String iStorageName) {
    String cacheClassName = CACHE_LEVEL2_IMPL.getValueAsString();
    try {
      Class<?> cacheClass = findByCanonicalName(cacheClassName);
      checkThatImplementsCacheInterface(cacheClass);
      Constructor<?> cons = getPublicConstructorWithLimitParameter(cacheClass);

      return (OCache) cons.newInstance(iStorageName, CACHE_LEVEL2_SIZE.getValueAsInteger());
    } catch (Exception e) {
      OLogManager.instance().error(this,
          "Cannot initialize cache with implementation class [%s]. %s. Using default implementation [%s]", cacheClassName,
          e.getMessage(), ODefaultCache.class.getCanonicalName());
    }
    return new ODefaultCache(null, CACHE_LEVEL2_SIZE.getValueAsInteger());
  }

  private void checkThatImplementsCacheInterface(final Class<?> cacheClass) {
    if (!OCache.class.isAssignableFrom(cacheClass))
      throw new IllegalArgumentException("Class " + cacheClass.getCanonicalName() + " doesn't implement "
          + OCache.class.getCanonicalName() + " interface");
  }

  private Class<?> findByCanonicalName(final String cacheClassName) {
    try {
      return Class.forName(cacheClassName);
    } catch (ClassNotFoundException e) {
      throw new IllegalArgumentException("Class not found", e);
    }
  }

  private Constructor<?> getPublicConstructorWithLimitParameter(final Class<?> cacheClass) {
    try {
      return cacheClass.getConstructor(String.class, int.class);
    } catch (NoSuchMethodException e) {
      throw new IllegalArgumentException("Class has no public constructor with parameter of type [" + String.class + ","
          + int.class + "]", e);
    }
  }
}
