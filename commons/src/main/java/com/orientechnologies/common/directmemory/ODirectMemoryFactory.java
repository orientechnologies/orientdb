/*
 * Copyright 1999-2012 Luca Garulli (l.garulli--at--orientechnologies.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.orientechnologies.common.directmemory;

import com.orientechnologies.common.log.OLogManager;

/**
 * @author Andrey Lomakin
 * @since 2/11/13
 */
class ODirectMemoryFactory {
  public static final ODirectMemoryFactory INSTANCE = new ODirectMemoryFactory();

  private static final ODirectMemory       directMemory;
  static {
    ODirectMemory localDirectMemory = null;
    try {
      final Class<?> jnaClass = Class.forName("com.orientechnologies.nio.OJNADirectMemory");
      if (jnaClass == null)
        localDirectMemory = null;
      else
        localDirectMemory = (ODirectMemory) jnaClass.newInstance();
    } catch (Exception e) {
      // ignore
    }

    if (localDirectMemory == null) {
      try {
        final Class<?> sunClass = Class.forName("sun.misc.Unsafe");

        if (sunClass != null) {
          localDirectMemory = OUnsafeMemory.INSTANCE;
          OLogManager.instance().warn(
              ODirectMemoryFactory.class,
              "Sun Unsafe direct  memory implementation is going to be used, "
                  + "this implementation is not stable so please use JNA version instead.");
        }
      } catch (Exception e) {
        // ignore
      }
    }

    directMemory = localDirectMemory;
  }

  public ODirectMemory directMemory() {
    return directMemory;
  }
}
