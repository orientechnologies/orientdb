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

import java.util.HashMap;
import java.util.Map;

public class ODynamicFactoryInverse<K, V> extends ODynamicFactory<K, V> {
  protected Map<V, K> inverseRegistry = new HashMap<V, K>();

  public K getInverse(V iValue) {
    return inverseRegistry.get(iValue);
  }

  @Override
  public void register(K iKey, V iValue) {
    super.register(iKey, iValue);
    inverseRegistry.put(iValue, iKey);
  }

  @Override
  public void unregister(K iKey) {
    V value = get(iKey);
    if (value == null)
      return;
    super.unregister(iKey);
    inverseRegistry.remove(value);
  }
}
