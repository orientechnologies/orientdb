package com.orientechnologies.common.jna;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.util.OMemory;
import com.sun.jna.Native;
import com.sun.jna.Platform;

import javax.management.*;
import java.lang.management.ManagementFactory;

public class ONative {
  private static final OCLibrary C_LIBRARY;

  static {
    if (Platform.isLinux()) {
      C_LIBRARY = Native.loadLibrary("c", OCLibrary.class);
    } else {
      C_LIBRARY = null;
    }
  }

  public static ONative instance() {
    return InstanceHolder.INSTANCE;
  }

  public boolean isLinux() {
    return Platform.isLinux();
  }

  /**
   * @return PID if current functionality supported on running platform.
   */
  public long getPid() {
    if (C_LIBRARY != null) {
      return C_LIBRARY.getpid();
    } else {
      final String jvmName = ManagementFactory.getRuntimeMXBean().getName();
      final int index = jvmName.indexOf('@');

      if (index < 1) {
        return -1;
      } else {
        try {
          return Long.parseLong(jvmName.substring(0, index));
        } catch (NumberFormatException e) {
          return -1;
        }
      }
    }
  }

  /**
   * @return Amount of memory which are allowed to be consumed by application.
   */
  public long getMemoryLimit() {
    long memoryLimit = 0;

    if (Platform.isLinux()) {
      final OCLibrary.Rlimit rlimit = new OCLibrary.Rlimit();
      C_LIBRARY.getrlimit(OCLibrary.RLIMIT_AS, rlimit);

      memoryLimit = rlimit.rlim_cur;
    }

    if (memoryLimit <= 0) {
      memoryLimit = getPhysicalMemorySize();
    }

    return memoryLimit;
  }

  /**
   * Obtains the total size in bytes of the installed physical memory on this machine. Note that on some VMs it's impossible to
   * obtain the physical memory size, in this case the return value will {@code -1}.
   *
   * @return the total physical memory size in bytes or {@code -1} if the size can't be obtained.
   */
  private long getPhysicalMemorySize() {
    long osMemory = -1;

    try {
      MBeanServer mBeanServer = ManagementFactory.getPlatformMBeanServer();
      Object attribute = mBeanServer
          .getAttribute(new ObjectName("java.lang", "type", "OperatingSystem"), "TotalPhysicalMemorySize");

      if (attribute != null) {
        if (attribute instanceof Long) {
          osMemory = (Long) attribute;
        } else {
          try {
            osMemory = Long.parseLong(attribute.toString());
          } catch (NumberFormatException e) {
            if (!OLogManager.instance().isDebugEnabled())
              OLogManager.instance().warn(OMemory.class, "Unable to determine the amount of installed RAM.");
            else
              OLogManager.instance().debug(OMemory.class, "Unable to determine the amount of installed RAM.", e);
          }
        }
      } else {
        if (!OLogManager.instance().isDebugEnabled())
          OLogManager.instance().warn(OMemory.class, "Unable to determine the amount of installed RAM.");
      }
    } catch (MalformedObjectNameException e) {
      if (!OLogManager.instance().isDebugEnabled())
        OLogManager.instance().warn(OMemory.class, "Unable to determine the amount of installed RAM.");
      else
        OLogManager.instance().debug(OMemory.class, "Unable to determine the amount of installed RAM.", e);
    } catch (AttributeNotFoundException e) {
      if (!OLogManager.instance().isDebugEnabled())
        OLogManager.instance().warn(OMemory.class, "Unable to determine the amount of installed RAM.");
      else
        OLogManager.instance().debug(OMemory.class, "Unable to determine the amount of installed RAM.", e);
    } catch (InstanceNotFoundException e) {
      if (!OLogManager.instance().isDebugEnabled())
        OLogManager.instance().warn(OMemory.class, "Unable to determine the amount of installed RAM.");
      else
        OLogManager.instance().debug(OMemory.class, "Unable to determine the amount of installed RAM.", e);
    } catch (MBeanException e) {
      if (!OLogManager.instance().isDebugEnabled())
        OLogManager.instance().warn(OMemory.class, "Unable to determine the amount of installed RAM.");
      else
        OLogManager.instance().debug(OMemory.class, "Unable to determine the amount of installed RAM.", e);
    } catch (ReflectionException e) {
      if (!OLogManager.instance().isDebugEnabled())
        OLogManager.instance().warn(OMemory.class, "Unable to determine the amount of installed RAM.");
      else
        OLogManager.instance().debug(OMemory.class, "Unable to determine the amount of installed RAM.", e);
    } catch (RuntimeException e) {
      OLogManager.instance().warn(OMemory.class, "Unable to determine the amount of installed RAM.", e);
    }

    return osMemory;
  }

  private static class InstanceHolder {
    private static final ONative INSTANCE;

    static {
      INSTANCE = new ONative();
    }
  }
}
