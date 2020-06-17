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

/**
 * Structure to handle a triple of values configured as a key and a Pair as value.
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 * @see OPair
 */
public class OTriple<K extends Comparable<K>, V extends Comparable<V>, SV>
    implements Comparable<OTriple<K, V, SV>> {
  public K key;
  public OPair<V, SV> value;

  public OTriple() {}

  public OTriple(final K iKey, final V iValue, final SV iSubValue) {
    init(iKey, iValue, iSubValue);
  }

  public void init(final K iKey, final V iValue, final SV iSubValue) {
    key = iKey;
    value = new OPair(iValue, iSubValue);
  }

  public K getKey() {
    return key;
  }

  public OPair<V, SV> getValue() {
    return value;
  }

  public OPair<V, SV> setValue(final OPair<V, SV> iValue) {
    final OPair<V, SV> oldValue = value;
    value = iValue;
    return oldValue;
  }

  public void setSubValue(final SV iSubValue) {
    final OPair<V, SV> oldValue = value;
    value.setValue(iSubValue);
  }

  @Override
  public String toString() {
    return key + ":" + value.getKey() + "/" + value.getValue();
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
    if (this == obj) return true;
    if (obj == null) return false;
    if (getClass() != obj.getClass()) return false;
    OTriple<?, ?, ?> other = (OTriple<?, ?, ?>) obj;
    if (key == null) {
      if (other.key != null) return false;
    } else if (!key.equals(other.key)) return false;
    return true;
  }

  public int compareTo(final OTriple<K, V, SV> o) {
    return key.compareTo(o.key);
  }
}
