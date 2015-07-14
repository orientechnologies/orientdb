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
package com.orientechnologies.common.util;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

/**
 * Keeps a pair of values as Key/Value.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 * @param <K>
 *          Key
 * @param <V>
 *          Value
 * @see OTriple
 */
public class OPair<K extends Comparable<K>, V> implements Entry<K, V>, Comparable<OPair<K, V>>, Serializable {
  public K key;
  public V value;

  public OPair() {
  }

  public OPair(final K iKey, final V iValue) {
    key = iKey;
    value = iValue;
  }

  public OPair(final Entry<K, V> iSource) {
    key = iSource.getKey();
    value = iSource.getValue();
  }

  public void init(final K iKey, final V iValue) {
    key = iKey;
    value = iValue;
  }

  public K getKey() {
    return key;
  }

  public V getValue() {
    return value;
  }

  public V setValue(final V iValue) {
    V oldValue = value;
    value = iValue;
    return oldValue;
  }

  @Override
  public String toString() {
    final StringBuilder buffer = new StringBuilder(512);
    buffer.append(key);
    buffer.append(':');

    if (value == null || !value.getClass().isArray())
      buffer.append(value);
    else
      buffer.append(Arrays.toString((Object[]) value));

    return buffer.toString();
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((key == null) ? 0 : key.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    OPair<?, ?> other = (OPair<?, ?>) obj;
    if (key == null) {
      if (other.key != null)
        return false;
    } else if (!key.equals(other.key))
      return false;
    return true;
  }

  public int compareTo(final OPair<K, V> o) {
    return key.compareTo(o.key);
  }

  public static <K extends Comparable<K>, V> Map<K, V> convertToMap(final List<OPair<K, V>> iValues) {
    final HashMap<K, V> result = new HashMap<K, V>(iValues.size());
    for (OPair<K, V> p : iValues)
      result.put(p.getKey(), p.getValue());

    return result;
  }

  public static <K extends Comparable<K>, V> List<OPair<K, V>> convertFromMap(final Map<K, V> iValues) {
    final List<OPair<K, V>> result = new ArrayList<OPair<K, V>>(iValues.size());
    for (Entry<K, V> p : iValues.entrySet())
      result.add(new OPair<K, V>(p.getKey(), p.getValue()));

    return result;
  }
}
