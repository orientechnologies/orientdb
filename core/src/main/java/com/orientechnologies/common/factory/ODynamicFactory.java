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
package com.orientechnologies.common.factory;

import java.util.LinkedHashMap;
import java.util.Map;

public class ODynamicFactory<K, V> {
  protected final Map<K, V> registry = new LinkedHashMap<K, V>();

  public V get(final K iKey) {
    return registry.get(iKey);
  }

  public void register(final K iKey, final V iValue) {
    registry.put(iKey, iValue);
  }

  public void unregister(final K iKey) {
    registry.remove(iKey);
  }

  public void unregisterAll() {
    registry.clear();
  }
}
