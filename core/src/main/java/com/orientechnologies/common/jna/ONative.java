package com.orientechnologies.common.jna;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.util.OMemory;
import com.sun.jna.Native;
import com.sun.jna.Platform;

import javax.management.*;
import java.io.*;
import java.lang.management.ManagementFactory;

public class ONative {
  private static final OCLibrary C_LIBRARY;
  private static final String DEFAULT_MEMORY_CGROUP_PATH = "/sys/fs/memory";

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

  /**
   * @param printSteps Print all steps of discovering of memory limit in the log with {@code INFO} level.
   *
   * @return Amount of memory which are allowed to be consumed by application, if value <= 0 then limit is not set.
   */
  public long getMemoryLimit(boolean printSteps) {
    long memoryLimit = getPhysicalMemorySize();

    if (memoryLimit > 0 && printSteps) {
      OLogManager.instance().info(this, "%d bytes of physical memory were detected on machine", memoryLimit);
    }

    if (Platform.isLinux()) {
      final OCLibrary.Rlimit rlimit = new OCLibrary.Rlimit();
      int result = C_LIBRARY.getrlimit(OCLibrary.RLIMIT_AS, rlimit);

      if (result == 0 && rlimit.rlim_cur > 0) {
        if (printSteps)
          OLogManager.instance().info(this, "Soft memory limit for this process is set to %d bytes", rlimit.rlim_cur);

        if (rlimit.rlim_cur < memoryLimit || memoryLimit <= 0) {
          memoryLimit = rlimit.rlim_cur;
        }

      }

      String memoryCGroupPath = null;

      final File cgroupList = new File("/proc/self/cgroup");
      if (cgroupList.exists()) {
        try {
          final FileReader cgroupListReader = new FileReader(cgroupList);
          final BufferedReader bufferedCGroupReader = new BufferedReader(cgroupListReader);
          try {
            String cgroupData;
            try {
              while ((cgroupData = bufferedCGroupReader.readLine()) != null) {
                final String[] cgroupParts = cgroupData.split(":");
                if (cgroupParts[1].equals("memory")) {
                  memoryCGroupPath = cgroupParts[2];
                }
              }
            } catch (IOException ioe) {
              OLogManager.instance().error(this, "Error during reading of details of list of cgroups for the current process, "
                  + "no restrictions applied by cgroups will be taken into account", ioe);
              memoryCGroupPath = null;
            }

          } finally {
            try {
              bufferedCGroupReader.close();
            } catch (IOException ioe) {
              OLogManager.instance()
                  .error(this, "Error during closing of reader which reads details of list of cgroups for the current process",
                      ioe);
            }
          }
        } catch (FileNotFoundException fnfe) {
          OLogManager.instance().warn(this, "Can not retrieve list of cgroups to which process belongs, "
              + "no restrictions applied by cgroups will be taken into account");
        }
      }

      if (memoryCGroupPath != null) {
        if (printSteps)
          OLogManager.instance().info(this, "Path to 'memory' cgroup is %s, reading memory limits", memoryCGroupPath);

        String memoryCGroupRoot = null;

        //find all mounting points
        final File procMounts = new File("/proc/mounts");
        if (procMounts.exists()) {
          FileReader mountsReader;
          try {
            mountsReader = new FileReader(procMounts);
            BufferedReader bufferedMountsReader = new BufferedReader(mountsReader);

            try {
              String fileSystem;
              while ((fileSystem = bufferedMountsReader.readLine()) != null) {
                final String[] fsParts = fileSystem.split("\\s+");
                if (fsParts.length == 0) {
                  continue;
                }

                final String fsType = fsParts[0];
                //all cgroup controllers have "cgroup" as file system type
                if (fsType.equals("cgroup")) {
                  //get mounting path of cgroup
                  final String fsMountingPath = fsParts[1];
                  final String[] fsPathParts = fsMountingPath.split("/");
                  if (fsPathParts[fsPathParts.length - 1].equals("memory")) {
                    memoryCGroupRoot = fsMountingPath;
                  }
                }
              }
            } catch (IOException e) {
              OLogManager.instance().error(this, "Error during reading a list of mounted file systems", e);
              memoryCGroupRoot = DEFAULT_MEMORY_CGROUP_PATH;
            } finally {
              try {
                bufferedMountsReader.close();
              } catch (IOException e) {
                OLogManager.instance().error(this, "Error during closing of reader of list of mounted file systems", e);
              }
            }

          } catch (FileNotFoundException fnfe) {
            memoryCGroupRoot = DEFAULT_MEMORY_CGROUP_PATH;
          }
        }

        if (memoryCGroupRoot == null) {
          memoryCGroupRoot = DEFAULT_MEMORY_CGROUP_PATH;
        }

        if (printSteps)
          OLogManager.instance().info(this, "Mounting path for memory cgroup controller is %s", memoryCGroupRoot);

        File memoryCGroup = new File(memoryCGroupRoot, memoryCGroupPath);
        if (!memoryCGroup.exists()) {
          if (printSteps)
            OLogManager.instance().info(this, "Can not find '%s' path for memory cgroup, it is supposed that "
                + "process is running in container, will try to read root '%s' memory cgroup data", memoryCGroup, memoryCGroupRoot);

          memoryCGroup = new File(memoryCGroupRoot);
        }

        boolean readHardLimit = false;
        File softMemoryCGroupLimit = new File(memoryCGroup, "memory.soft_limit_in_bytes");
        if (softMemoryCGroupLimit.exists()) {
          try {
            final FileReader memoryLimitReader = new FileReader(softMemoryCGroupLimit);
            final BufferedReader bufferedMemoryLimitReader = new BufferedReader(memoryLimitReader);
            try {
              try {
                final String cgroupMemoryLimitValueStr = bufferedMemoryLimitReader.readLine();
                try {
                  final long cgroupMemoryLimitValue = Long.parseLong(cgroupMemoryLimitValueStr);

                  if (printSteps)
                    OLogManager.instance().info(this, "cgroup soft memory limit is %d", cgroupMemoryLimitValue);

                  if (cgroupMemoryLimitValue < memoryLimit) {
                    memoryLimit = cgroupMemoryLimitValue;
                  }
                } catch (NumberFormatException nfe) {
                  OLogManager.instance()
                      .error(this, "Can not read memory soft limit for cgroup %s, will try to read memory hard limit", nfe,
                          memoryCGroup);
                  readHardLimit = true;
                }
              } catch (IOException ioe) {
                OLogManager.instance()
                    .error(this, "Can not read memory soft limit for cgroup %s, will try to read memory hard limit", ioe,
                        memoryCGroup);
                readHardLimit = true;
              }
            } finally {
              try {
                bufferedMemoryLimitReader.close();
              } catch (IOException ioe) {
                OLogManager.instance().error(this, "Error on closing the reader of soft memory limit", ioe);
              }
            }
          } catch (FileNotFoundException fnfe) {
            OLogManager.instance()
                .error(this, "Can not read memory soft limit for cgroup %s, will try to read memory hard limit", fnfe,
                    memoryCGroup);
            readHardLimit = true;
          }
        } else {
          if (printSteps)
            OLogManager.instance()
                .info(this, "Can not read memory soft limit for cgroup %s, will try to read memory hard limit", memoryCGroup);

          readHardLimit = true;
        }

        if (readHardLimit) {
          final File hardMemoryCGroupLimit = new File(memoryCGroup, "memory.limit_in_bytes");
          if (hardMemoryCGroupLimit.exists()) {
            try {
              final FileReader memoryLimitReader = new FileReader(softMemoryCGroupLimit);
              final BufferedReader bufferedMemoryLimitReader = new BufferedReader(memoryLimitReader);
              try {
                try {
                  final String cgroupMemoryLimitValueStr = bufferedMemoryLimitReader.readLine();
                  try {
                    final long cgroupMemoryLimitValue = Long.parseLong(cgroupMemoryLimitValueStr);

                    if (printSteps)
                      OLogManager.instance().info(this, "cgroup hard memory limit is %d", cgroupMemoryLimitValue);

                    if (cgroupMemoryLimitValue < memoryLimit) {
                      memoryLimit = cgroupMemoryLimitValue;
                    }
                  } catch (NumberFormatException nfe) {
                    OLogManager.instance().error(this,
                        "Can not read memory hard limit for cgroup %s, cgroup memory limits for current "
                            + "process will not be applied", nfe, memoryCGroup);
                  }
                } catch (IOException ioe) {
                  OLogManager.instance().error(this,
                      "Can not read memory hard limit for cgroup %s, cgroup memory limits for current process"
                          + " will not be applied", ioe, memoryCGroup);
                }
              } finally {
                try {
                  bufferedMemoryLimitReader.close();
                } catch (IOException ioe) {
                  OLogManager.instance().error(this, "Error on closing the reader of hard memory limit", ioe);
                }
              }
            } catch (FileNotFoundException fnfe) {
              OLogManager.instance().error(this,
                  "Can not read memory hard limit for cgroup %s, cgroup memory limits for current process will not be applied",
                  fnfe, memoryCGroup);
            }
          } else {
            if (printSteps) {
              OLogManager.instance().info(this,
                  "Can not read memory hard limit for cgroup %s, cgroup memory limits for current process will not be applied",
                  memoryCGroup);
            }
          }
        }
      }
    }

    if (printSteps) {
      if (memoryLimit > 0)
        OLogManager.instance().info(this, "Detected memory limit for current process is %d", memoryLimit);
      else
        OLogManager.instance().info(this, "Memory limit for current process is not set");
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
