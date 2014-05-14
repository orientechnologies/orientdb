/*
 *
 *  Copyright 2010-2014 Orient Technologies LTD (info(at)orientechnologies.com)
 *  
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *  
 *       http://www.apache.org/licenses/LICENSE-2.0
 *  
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *  
 */
package com.orientechnologies.common.factory;

import com.orientechnologies.common.exception.OException;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Configurable stateful factory. New instances are created when newInstance() is called, invoking its default empty constructor.
 * 
 * @param <K>
 *          Factory key
 * @param <V>
 *          Instance type
 */
public class OConfigurableStatefulFactory<K, V> {
  protected final Map<K, Class<? extends V>> registry = new LinkedHashMap<K, Class<? extends V>>();
  protected V                                defaultClass;

  public Class<? extends V> get(final K iKey) {
    return registry.get(iKey);
  }

  public V newInstance(final K iKey) throws IllegalAccessException {
    final Class<? extends V> instance = registry.get(iKey);
    if (instance != null) {
      try {
        return instance.newInstance();
      } catch (Exception e) {
        throw new OException(String.format("Error on creating new instance of class '%s' registered in factory with key '%s'",
            instance, iKey), e);
      }
    }

    return defaultClass;
  }

  public OConfigurableStatefulFactory<K, V> register(final K iKey, final Class<? extends V> iValue) {
    registry.put(iKey, iValue);
    return this;
  }

  public OConfigurableStatefulFactory<K, V> unregister(final K iKey) {
    registry.remove(iKey);
    return this;
  }

  public OConfigurableStatefulFactory<K, V> unregisterAll() {
    registry.clear();
    return this;
  }

  public V getDefaultClass() {
    return defaultClass;
  }

  public <C extends V> OConfigurableStatefulFactory<K, V> setDefaultClass(final C defaultClass) {
    this.defaultClass = defaultClass;
    return this;
  }
}
