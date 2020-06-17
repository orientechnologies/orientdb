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

import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import java.util.Comparator;

/**
 * Creates comparators for classes that does not implement {@link Comparable} but logically can be
 * compared.
 *
 * @author Andrey Lomakin (a.lomakin-at-orientdb.com)
 * @since 03.07.12
 */
public class OComparatorFactory {
  public static final OComparatorFactory INSTANCE = new OComparatorFactory();

  private static final boolean unsafeWasDetected;

  static {
    boolean unsafeDetected = false;

    try {
      Class<?> sunClass = Class.forName("sun.misc.Unsafe");
      unsafeDetected = sunClass != null;
    } catch (ClassNotFoundException ignore) {
      // Ignore
    }

    unsafeWasDetected = unsafeDetected;
  }

  /**
   * Returns {@link Comparator} instance if applicable one exist or <code>null</code> otherwise.
   *
   * @param clazz Class of object that is going to be compared.
   * @param <T> Class of object that is going to be compared.
   * @return {@link Comparator} instance if applicable one exist or <code>null</code> otherwise.
   */
  @SuppressWarnings("unchecked")
  public <T> Comparator<T> getComparator(Class<T> clazz) {
    boolean useUnsafe = OGlobalConfiguration.MEMORY_USE_UNSAFE.getValueAsBoolean();

    if (clazz.equals(byte[].class)) {
      if (useUnsafe && unsafeWasDetected)
        return (Comparator<T>) OUnsafeByteArrayComparator.INSTANCE;

      return (Comparator<T>) OByteArrayComparator.INSTANCE;
    }

    return null;
  }
}
