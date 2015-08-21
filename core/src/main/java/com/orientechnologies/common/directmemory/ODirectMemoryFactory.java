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
package com.orientechnologies.common.directmemory;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.nio.OJNADirectMemory;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.sun.jna.Platform;

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
      boolean needToUseUnsafe = false;
      if (Platform.isAIX()) {
        OLogManager.instance().warn(ODirectMemoryFactory.class,
            "System is running on AIX OS, automatically switch to usage of Unsafe class");
        needToUseUnsafe = true;
      }

      needToUseUnsafe |= OGlobalConfiguration.MEMORY_USE_UNSAFE.getValueAsBoolean();

      if (needToUseUnsafe) {
        final Class<?> sunClass = Class.forName("sun.misc.Unsafe");

        if (sunClass != null)
          localDirectMemory = OUnsafeMemory.INSTANCE;
      }

    } catch (Throwable e) {
      // ignore
    }

    if (localDirectMemory == null)
      localDirectMemory = new OJNADirectMemory();

    directMemory = localDirectMemory;
  }

  public ODirectMemory directMemory() {
    return directMemory;
  }
}
