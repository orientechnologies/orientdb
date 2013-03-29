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
import com.orientechnologies.orient.core.storage.impl.local.paginated.OLocalPaginatedStorage;

/**
 * @author Andrey Lomakin
 * @since 28.03.13
 */
public class OEngineLocalPaginated extends OEngineAbstract {
  public static final String         NAME         = "plocal";
  private static final AtomicBoolean memoryLocked = new AtomicBoolean(false);

  public OStorage createStorage(final String dbName, final Map<String, String> configuration) {
    if (memoryLocked.compareAndSet(false, true)) {
      lockMemory();
    }

    try {
      // GET THE STORAGE
      return new OLocalPaginatedStorage(dbName, dbName, getMode(configuration));

    } catch (Throwable t) {
      OLogManager.instance().error(this,
          "Error on opening database: " + dbName + ". Current location is: " + new java.io.File(".").getAbsolutePath(), t,
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
      } catch (NoSuchMethodException e) {
        throw new OMemoryLockException("Error while locking memory", e);
      } catch (InvocationTargetException e) {
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
