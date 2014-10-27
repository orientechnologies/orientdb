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
package com.orientechnologies.common.collection;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Implementation of {@link LinkedHashMap} that will remove eldest entries if size limit will be exceeded.
 * 
 * @author Luca Garulli
 */
@SuppressWarnings("serial")
public class OLimitedMap<K, V> extends LinkedHashMap<K, V> {
  protected final int limit;

  public OLimitedMap(final int initialCapacity, final float loadFactor, final int limit) {
    super(initialCapacity, loadFactor, true);
    this.limit = limit;
  }

  @Override
  protected boolean removeEldestEntry(final Map.Entry<K, V> eldest) {
    return limit > 0 ? size() - limit > 0 : false;
  }
}
