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
package com.orientechnologies.common.comparator;

import java.util.Comparator;

/**
 * Comparator that calls {@link Comparable#compareTo(Object)} methods for getting results for all {@link Comparable} types.
 * Otherwise result of {@link Comparator} that returned from {@link OComparatorFactory} will be used.
 * 
 * The special case is null values. Null is treated as smallest value against other values. If both arguments are null they are
 * treated as equal.
 * 
 * @author Andrey Lomakin
 * @since 03.07.12
 */
public class ODefaultComparator implements Comparator<Object> {
  public static final ODefaultComparator INSTANCE = new ODefaultComparator();

  @SuppressWarnings("unchecked")
  public int compare(final Object objectOne, final Object objectTwo) {
    if (objectOne == null) {
      if (objectTwo == null)
        return 0;
      else
        return -1;
    } else if (objectTwo == null)
      return 1;

    if (objectOne == objectTwo)
      // FAST COMPARISON
      return 0;

    if (objectOne instanceof Comparable)
      return ((Comparable<Object>) objectOne).compareTo(objectTwo);

    final Comparator<?> comparator = OComparatorFactory.INSTANCE.getComparator(objectOne.getClass());

    if (comparator != null)
      return ((Comparator<Object>) comparator).compare(objectOne, objectTwo);

    throw new IllegalStateException("Object of class '" + objectOne.getClass().getName() + "' cannot be compared");
  }
}
