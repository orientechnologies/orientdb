/*
 *
 *  Copyright 2010-2014 OrientDB LTD (info(-at-)orientdb.com)
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

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Configurable stateless factory. The registered instances must not contain any state, so they can
 * be reused even concurrently.
 *
 * @param <K> Factory key
 * @param <V> Instance type
 */
public class OConfigurableStatelessFactory<K, V> {
  private final Map<K, V> registry = new HashMap<K, V>();
  private V defaultImplementation;

  public V getImplementation(final K iKey) {
    if (iKey == null) return defaultImplementation;
    return registry.get(iKey);
  }

  public Set<K> getRegisteredImplementationNames() {
    return registry.keySet();
  }

  public OConfigurableStatelessFactory<K, V> registerImplementation(final K iKey, final V iValue) {
    registry.put(iKey, iValue);
    return this;
  }

  public OConfigurableStatelessFactory<K, V> unregisterImplementation(final K iKey) {
    registry.remove(iKey);
    return this;
  }

  public OConfigurableStatelessFactory<K, V> unregisterAllImplementations() {
    registry.clear();
    return this;
  }

  public V getDefaultImplementation() {
    return defaultImplementation;
  }

  public OConfigurableStatelessFactory<K, V> setDefaultImplementation(final V defaultImpl) {
    this.defaultImplementation = defaultImpl;
    return this;
  }
}
