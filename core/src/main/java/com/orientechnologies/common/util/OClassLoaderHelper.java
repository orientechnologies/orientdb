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

package com.orientechnologies.common.util;

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.exception.OConfigurationException;
import java.util.Iterator;
import java.util.ServiceLoader;

public class OClassLoaderHelper {

  /**
   * Switch to the OrientDb classloader before lookups on ServiceRegistry for implementation of the
   * given Class. Useful under OSGI and generally under applications where jars are loaded by
   * another class loader
   *
   * @param clazz the class to lookup foor
   * @return an Iterator on the class implementation
   */
  public static synchronized <T extends Object> Iterator<T> lookupProviderWithOrientClassLoader(
      Class<T> clazz) {

    return lookupProviderWithOrientClassLoader(clazz, OClassLoaderHelper.class.getClassLoader());
  }

  public static synchronized <T extends Object> Iterator<T> lookupProviderWithOrientClassLoader(
      Class<T> clazz, ClassLoader orientClassLoader) {

    final ClassLoader origClassLoader = Thread.currentThread().getContextClassLoader();
    Thread.currentThread().setContextClassLoader(orientClassLoader);
    try {
      return ServiceLoader.load(clazz).iterator();
    } catch (Exception e) {
      OLogManager.instance().warn(null, "Cannot lookup in service registry", e);
      throw OException.wrapException(
          new OConfigurationException("Cannot lookup in service registry"), e);
    } finally {
      Thread.currentThread().setContextClassLoader(origClassLoader);
    }
  }
}
