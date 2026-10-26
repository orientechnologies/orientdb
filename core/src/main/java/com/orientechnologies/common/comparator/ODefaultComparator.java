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
package com.orientechnologies.common.comparator;

import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Map;

/**
 * Comparator that calls {@link Comparable#compareTo(Object)} methods for getting results for all
 * {@link Comparable} types. Otherwise result of {@link Comparator} that returned from {@link
 * OComparatorFactory} will be used.
 *
 * <p>The special case is null values. Null is treated as smallest value against other values. If
 * both arguments are null they are treated as equal.
 *
 * @author Andrey Lomakin (a.lomakin-at-orientdb.com)
 * @since 03.07.12
 */
public class ODefaultComparator implements Comparator<Object> {
  public static final ODefaultComparator INSTANCE = new ODefaultComparator();

  @SuppressWarnings("unchecked")
  public int compare(final Object objectOne, final Object objectTwo) {
    if (objectOne == null) {
      if (objectTwo == null) return 0;
      else return -1;
    } else if (objectTwo == null) return 1;

    if (objectOne == objectTwo)
      // FAST COMPARISON
      return 0;

    if (objectOne instanceof Comparable)
      return ((Comparable<Object>) objectOne).compareTo(objectTwo);
    if (objectOne instanceof Collection && objectTwo instanceof Collection)
      return compareCollection((Collection) objectOne, (Collection) objectTwo);
    if (objectOne instanceof Map && objectTwo instanceof Map)
      return compareMap((Map) objectOne, (Map) objectTwo);

    final Comparator<?> comparator =
        OComparatorFactory.INSTANCE.getComparator(objectOne.getClass());

    if (comparator != null) return ((Comparator<Object>) comparator).compare(objectOne, objectTwo);

    throw new IllegalStateException(
        "Object of class '" + objectOne.getClass().getName() + "' cannot be compared");
  }

  private int compareMap(Map objectOne, Map objectTwo) {
    Iterator<Map.Entry> it = objectOne.entrySet().iterator();
    Iterator<Map.Entry> ito = objectTwo.entrySet().iterator();
    Comparator<Object> cmp = ODefaultComparator.INSTANCE;
    while (it.hasNext() && ito.hasNext()) {
      var next = it.next();
      var nexto = ito.next();
      var keyc = cmp.compare(next.getKey(), nexto.getKey());
      if (keyc == 0) {
        var valuec = cmp.compare(next.getValue(), nexto.getValue());
        if (valuec != 0) {
          return valuec;
        }
      } else {
        return keyc;
      }
    }
    if (it.hasNext()) {
      return 1;
    } else if (ito.hasNext()) {
      return -1;
    } else {
      return 0;
    }
  }

  private int compareCollection(Collection objectOne, Collection objectTwo) {
    Iterator<Object> it = objectOne.iterator();
    Iterator<Object> ito = objectTwo.iterator();
    Comparator<Object> cmp = ODefaultComparator.INSTANCE;
    while (it.hasNext() && ito.hasNext()) {
      var next = it.next();
      var nexto = ito.next();
      var valuec = cmp.compare(next, nexto);
      if (valuec != 0) {
        return valuec;
      }
    }
    if (it.hasNext()) {
      return 1;
    } else if (ito.hasNext()) {
      return -1;
    } else {
      return 0;
    }
  }
}
