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

import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

/** Set of utility methods to work with collections. */
public class OCollections {
  /**
   * This method is used to find item in collection using passed in comparator. Only 0 value
   * (requested object is found) returned by comparator is taken into account the rest is ignored.
   *
   * @param list List in which value should be found.
   * @param object Object to find.
   * @param comparator Comparator is sued for search.
   * @param <T> Type of collection elements.
   * @return Index of found item or <code>-1</code> otherwise.
   */
  public static <T> int indexOf(
      final List<T> list, final T object, final Comparator<T> comparator) {
    int i = 0;
    for (final T item : list) {
      if (comparator.compare(item, object) == 0) return i;
      i++;
    }
    return -1;
  }

  /**
   * This method is used to find an item in an array.
   *
   * @param array Array in which value should be found.
   * @param object Object to find.
   * @return Index of found item or <code>-1</code> otherwise.
   */
  public static int indexOf(final Object[] array, final Comparable object) {
    for (int i = 0; i < array.length; ++i) {
      if (object.compareTo(array[i]) == 0)
        // FOUND
        return i;
    }
    return -1;
  }

  /**
   * This method is used to find a number in an array.
   *
   * @param array Array of integers in which value should be found.
   * @param object number to find.
   * @return Index of found item or <code>-1</code> otherwise.
   */
  public static int indexOf(final int[] array, final int object) {
    for (int i = 0; i < array.length; ++i) {
      if (array[i] == object)
        // FOUND
        return i;
    }
    return -1;
  }

  /**
   * Create a string representation of all objects in the given Iterable. example :
   * [value1,value2,value3]
   *
   * @param iterable
   * @return String
   */
  public static String toString(Iterable<?> iterable) {
    final StringBuilder builder = new StringBuilder(512);
    builder.append('[');
    int cnt = 0;
    final Iterator<?> ite = iterable.iterator();
    while (ite.hasNext()) {
      if (cnt != 0) {
        builder.append(',');
      }
      cnt++;
      final Object obj = ite.next();
      builder.append(obj);
    }
    builder.append(']');
    return builder.toString();
  }
}
