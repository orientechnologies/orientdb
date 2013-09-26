/*
 * Copyright 2010-2012 Luca Garulli (l.garulli--at--orientechnologies.com)
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
package com.orientechnologies.orient.core.engine.local;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.engine.OEngineAbstract;
import com.orientechnologies.orient.core.exception.ODatabaseException;
import com.orientechnologies.orient.core.exception.OMemoryLockException;
import com.orientechnologies.orient.core.storage.OStorage;
import com.orientechnologies.orient.core.storage.impl.local.OStorageLocal;

public class OEngineLocal extends OEngineAbstract {

  public static final String         NAME         = "local";
  private static final AtomicBoolean memoryLocked = new AtomicBoolean(false);

  public OStorage createStorage(final String iDbName, final Map<String, String> iConfiguration) {

    if (memoryLocked.compareAndSet(false, true)) {
      lockMemory();
    }

    try {
      // GET THE STORAGE
      return new OStorageLocal(iDbName, iDbName, getMode(iConfiguration));

    } catch (Throwable t) {
      OLogManager.instance().error(this,
          "Error on opening database: " + iDbName + ". Current location is: " + new java.io.File(".").getAbsolutePath(), t,
          ODatabaseException.class);
    }
    return null;
  }

  private void lockMemory() {
    if (!OGlobalConfiguration.FILE_MMAP_USE_OLD_MANAGER.getValueAsBoolean()
        && OGlobalConfiguration.FILE_MMAP_LOCK_MEMORY.getValueAsBoolean()) {
      // lock memory
      try {
        Class<?> MemoryLocker = ClassLoader.getSystemClassLoader().loadClass("com.orientechnologies.nio.MemoryLocker");
        Method lockMemory = MemoryLocker.getMethod("lockMemory", boolean.class);
        lockMemory.invoke(null, OGlobalConfiguration.JNA_DISABLE_USE_SYSTEM_LIBRARY.getValueAsBoolean());
      } catch (ClassNotFoundException e) {
        OLogManager
            .instance()
            .config(
                null,
                "[OEngineLocal.createStorage] Cannot lock virtual memory, the orientdb-nativeos.jar is not in classpath or there is not a native implementation for the current OS: "
                    + System.getProperty("os.name") + " v." + System.getProperty("os.name"));
      } catch (InvocationTargetException e) {
        OLogManager
            .instance()
            .config(
                null,
                "[OEngineLocal.createStorage] Cannot lock virtual memory, the orientdb-nativeos.jar is not in classpath or there is not a native implementation for the current OS: "
                    + System.getProperty("os.name") + " v." + System.getProperty("os.name"));
      } catch (NoSuchMethodException e) {
        throw new OMemoryLockException("Error while locking memory", e);
      } catch (IllegalAccessException e) {
        throw new OMemoryLockException("Error while locking memory", e);
      }
    }
  }

  public String getName() {
    return NAME;
  }

  public boolean isShared() {
    return true;
  }
}
