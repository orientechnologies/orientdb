/*
 *
 *  *  Copyright 2010-2018 OrientDB LTD (http://orientdb.com)
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
package com.orientechnologies.common.jnr;

import com.orientechnologies.common.io.OIOUtils;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.log.OLogger;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.management.ManagementFactory;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import javax.management.AttributeNotFoundException;
import javax.management.InstanceNotFoundException;
import javax.management.MBeanException;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import javax.management.ReflectionException;
import jnr.constants.platform.Sysconf;
import jnr.posix.POSIX;
import jnr.posix.POSIXFactory;
import jnr.posix.RLimit;

public class ONative {
  private static final String DEFAULT_MEMORY_CGROUP_PATH = "/sys/fs/cgroup/memory";

  private static final OLogger logger = OLogManager.instance().logger(ONative.class);
  private static volatile ONative instance = null;
  private static final Lock initLock = new ReentrantLock();

  private static volatile POSIX posix;

  public static ONative instance() {
    if (instance != null) return instance;

    initLock.lock();
    try {
      if (instance != null) return instance;

      if (OIOUtils.isOsLinux()) {
        posix = POSIXFactory.getPOSIX();
      }

      instance = new ONative();
    } finally {
      initLock.unlock();
    }

    return instance;
  }

  /** Address space limit. */
  public static final int RLIMIT_AS = 9;

  public static final int RLIMIT_NOFILE = 7;

  /** Prevent initialization outside singleton */
  private ONative() {}

  /**
   * Detects limit of limit of open files.
   *
   * @param recommended recommended value of limit of open files.
   * @param defLimit default value for limit of open files.
   * @return limit of open files, available for the system.
   */
  public int getOpenFilesLimit(boolean verbose, int recommended, int defLimit) {
    if (OIOUtils.isOsLinux()) {
      try {
        final RLimit rLimit = posix.getrlimit(ONative.RLIMIT_NOFILE);
        if (rLimit.rlimCur() > 0) {
          if (verbose) {
            logger.infoNoDb(
                "Detected limit of amount of simultaneously open files is %d, "
                    + " limit of open files for disk cache will be set to %d",
                rLimit.rlimCur(), rLimit.rlimCur() / 2 - 512);
          }
          if (rLimit.rlimCur() < recommended) {
            logger.warnNoDb(
                "Value of limit of simultaneously open files is too small, recommended value is"
                    + " %d",
                recommended);
          }
          return (int) rLimit.rlimCur() / 2 - 512;
        } else {
          if (verbose) {
            logger.infoNoDb("Can not detect value of limit of open files.");
          }
        }
      } catch (final Exception e) {
        if (verbose) {
          logger.infoNoDb("Can not detect value of limit of open files.", e);
        }
      }
    } else if (OIOUtils.isOsWindows()) {
      if (verbose) {
        logger.infoNoDb(
            "Windows OS is detected, %d limit of open files will be set for the disk cache.",
            recommended);
      }
      return recommended;
    }

    if (verbose) {
      logger.infoNoDb("Default limit of open files (%d) will be used.", defLimit);
    }
    return defLimit;
  }

  /**
   * @param printSteps Print all steps of discovering of memory limit in the log with {@code INFO}
   *     level.
   * @return Amount of memory which are allowed to be consumed by application, and detects whether
   *     OrientDB instance is running inside container. If <code>null</code> is returned then it was
   *     impossible to detect amount of memory on machine.
   */
  public MemoryLimitResult getMemoryLimit(final boolean printSteps) {
    // Perform several steps here:
    // 1. Fetch physical size available on machine
    // 2. Fetch soft limit
    // 3. Fetch cgroup soft limit
    // 4. Fetch cgroup hard limit
    // 5. Return the minimal value from the list of results

    long memoryLimit = getPhysicalMemorySize();
    boolean insideContainer = false;

    if (printSteps) {
      logger.infoNoDb(
          "%d B/%d MB/%d GB of physical memory were detected on machine",
          memoryLimit, convertToMB(memoryLimit), convertToGB(memoryLimit));
    }

    if (OIOUtils.isOsLinux()) {
      try {
        final RLimit rLimit = posix.getrlimit(ONative.RLIMIT_AS);
        if (printSteps) {
          logger.infoNoDb(
              "Soft memory limit for this process is set to %d B/%d MB/%d GB",
              rLimit.rlimCur(), convertToMB(rLimit.rlimCur()), convertToGB(rLimit.rlimCur()));
        }
        memoryLimit = updateMemoryLimit(memoryLimit, rLimit.rlimCur());

        if (printSteps) {
          logger.infoNoDb(
              "Hard memory limit for this process is set to %d B/%d MB/%d GB",
              rLimit.rlimMax(), convertToMB(rLimit.rlimMax()), convertToGB(rLimit.rlimMax()));
        }
        memoryLimit = updateMemoryLimit(memoryLimit, rLimit.rlimMax());
      } catch (final Exception e) {
        if (printSteps) {
          logger.infoNoDb("Can not detect memory limit value.", e);
        }
      }

      final String memoryCGroupPath = findMemoryGCGroupPath();
      if (memoryCGroupPath != null) {
        if (printSteps) {
          logger.infoNoDb("Path to 'memory' cgroup is '%s'", memoryCGroupPath);
        }
        final String memoryCGroupRoot = findMemoryGCRoot();

        if (printSteps) {
          logger.infoNoDb("Mounting path for memory cgroup controller is '%s'", memoryCGroupRoot);
        }

        File memoryCGroup = new File(memoryCGroupRoot, memoryCGroupPath);
        if (!memoryCGroup.exists()) {
          if (printSteps) {
            logger.infoNoDb(
                "Can not find '%s' path for memory cgroup, it is supposed that process is"
                    + " running in container, will try to read root '%s' memory cgroup data",
                memoryCGroup, memoryCGroupRoot);
          }
          memoryCGroup = new File(memoryCGroupRoot);
          insideContainer = true;
        }

        final long softMemoryLimit = fetchCGroupSoftMemoryLimit(memoryCGroup, printSteps);
        memoryLimit = updateMemoryLimit(memoryLimit, softMemoryLimit);

        final long hardMemoryLimit = fetchCGroupHardMemoryLimit(memoryCGroup, printSteps);
        memoryLimit = updateMemoryLimit(memoryLimit, hardMemoryLimit);
      }
    }

    if (printSteps) {
      if (memoryLimit > 0)
        logger.infoNoDb(
            "Detected memory limit for current process is %d B/%d MB/%d GB",
            memoryLimit, convertToMB(memoryLimit), convertToGB(memoryLimit));
      else logger.infoNoDb("Memory limit for current process is not set");
    }
    if (memoryLimit <= 0) {
      return null;
    }
    return new MemoryLimitResult(memoryLimit, insideContainer);
  }

  public int getpagesize() throws LastErrorException {
    return (int) posix.sysconf(Sysconf._SC_PAGE_SIZE);
  }

  private long updateMemoryLimit(long memoryLimit, final long newMemoryLimit) {
    if (newMemoryLimit <= 0) {
      return memoryLimit;
    }

    if (memoryLimit <= 0) {
      memoryLimit = newMemoryLimit;
    }

    if (memoryLimit > newMemoryLimit) {
      memoryLimit = newMemoryLimit;
    }

    return memoryLimit;
  }

  private long fetchCGroupSoftMemoryLimit(final File memoryCGroup, final boolean printSteps) {
    final File softMemoryCGroupLimit = new File(memoryCGroup, "memory.soft_limit_in_bytes");
    if (softMemoryCGroupLimit.exists()) {
      try {
        final FileReader memoryLimitReader = new FileReader(softMemoryCGroupLimit);
        try (final BufferedReader bufferedMemoryLimitReader =
            new BufferedReader(memoryLimitReader)) {
          try {
            final String cgroupMemoryLimitValueStr = bufferedMemoryLimitReader.readLine();
            try {
              final long cgroupMemoryLimitValue = Long.parseLong(cgroupMemoryLimitValueStr);

              if (printSteps)
                logger.infoNoDb(
                    "cgroup soft memory limit is %d B/%d MB/%d GB",
                    cgroupMemoryLimitValue,
                    convertToMB(cgroupMemoryLimitValue),
                    convertToGB(cgroupMemoryLimitValue));

              return cgroupMemoryLimitValue;
            } catch (final NumberFormatException nfe) {
              if (cgroupMemoryLimitValueStr.matches("\\d+")) {
                if (printSteps) {
                  logger.infoNoDb("cgroup soft memory limit is not set");
                }
              } else {
                logger.errorNoDb(
                    "Can not read memory soft limit for cgroup '%s'", nfe, memoryCGroup);
              }
            }
          } catch (final IOException ioe) {
            logger.errorNoDb("Can not read memory soft limit for cgroup '%s'", ioe, memoryCGroup);
          }
        } catch (final IOException e) {
          logger.errorNoDb("Error on closing the reader of soft memory limit", e);
        }
      } catch (final FileNotFoundException fnfe) {
        logger.errorNoDb("Can not read memory soft limit for cgroup '%s'", fnfe, memoryCGroup);
      }
    } else {
      if (printSteps)
        logger.infoNoDb("Can not read memory soft limit for cgroup '%s'", memoryCGroup);
    }

    return -1;
  }

  private long fetchCGroupHardMemoryLimit(final File memoryCGroup, final boolean printSteps) {
    final File hardMemoryCGroupLimit = new File(memoryCGroup, "memory.limit_in_bytes");
    if (hardMemoryCGroupLimit.exists()) {
      try {
        final FileReader memoryLimitReader = new FileReader(hardMemoryCGroupLimit);

        try (final BufferedReader bufferedMemoryLimitReader =
            new BufferedReader(memoryLimitReader)) {
          try {
            final String cgroupMemoryLimitValueStr = bufferedMemoryLimitReader.readLine();
            try {
              final long cgroupMemoryLimitValue = Long.parseLong(cgroupMemoryLimitValueStr);

              if (printSteps)
                logger.infoNoDb(
                    "cgroup hard memory limit is %d B/%d MB/%d GB",
                    cgroupMemoryLimitValue,
                    convertToMB(cgroupMemoryLimitValue),
                    convertToGB(cgroupMemoryLimitValue));

              return cgroupMemoryLimitValue;
            } catch (final NumberFormatException nfe) {
              if (cgroupMemoryLimitValueStr.matches("\\d+")) {
                if (printSteps) {
                  logger.infoNoDb("cgroup hard memory limit is not set");
                }
              } else {
                logger.errorNoDb(
                    "Can not read memory hard limit for cgroup '%s'", nfe, memoryCGroup);
              }
            }
          } catch (final IOException ioe) {
            logger.errorNoDb("Can not read memory hard limit for cgroup '%s'", ioe, memoryCGroup);
          }
        } catch (final IOException e) {
          logger.errorNoDb("Error on closing the reader of hard memory limit", e);
        }
      } catch (final FileNotFoundException fnfe) {
        logger.errorNoDb("Can not read memory hard limit for cgroup '%s'", fnfe, memoryCGroup);
      }
    } else {
      if (printSteps) {
        logger.infoNoDb("Can not read memory hard limit for cgroup '%s'", memoryCGroup);
      }
    }

    return -1;
  }

  private String findMemoryGCGroupPath() {
    String memoryCGroupPath = null;

    // fetch list of cgroups to which given process belongs to
    final File cgroupList = new File("/proc/self/cgroup");
    if (cgroupList.exists()) {
      try {
        final FileReader cgroupListReader = new FileReader(cgroupList);

        try (final BufferedReader bufferedCGroupReader = new BufferedReader(cgroupListReader)) {
          String cgroupData;
          try {
            while ((cgroupData = bufferedCGroupReader.readLine()) != null) {
              final String[] cgroupParts = cgroupData.split(":");
              // we need only memory controller
              if (cgroupParts[1].equals("memory")) {
                memoryCGroupPath = cgroupParts[2];
              }
            }
          } catch (final IOException ioe) {
            logger.errorNoDb(
                "Error during reading of details of list of cgroups for the current process, "
                    + "no restrictions applied by cgroups will be taken into account",
                ioe);
            memoryCGroupPath = null;
          }

        } catch (final IOException e) {
          logger.errorNoDb(
              "Error during closing of reader which reads details of list of cgroups for the"
                  + " current process",
              e);
        }
      } catch (final FileNotFoundException fnfe) {
        logger.warnNoDb(
            "Can not retrieve list of cgroups to which process belongs, "
                + "no restrictions applied by cgroups will be taken into account");
      }
    }

    return memoryCGroupPath;
  }

  private String findMemoryGCRoot() {
    String memoryCGroupRoot = null;

    // fetch all mount points and find one to which cgroup memory controller is mounted
    final File procMounts = new File("/proc/mounts");
    if (procMounts.exists()) {
      final FileReader mountsReader;
      try {
        mountsReader = new FileReader(procMounts);
        try (BufferedReader bufferedMountsReader = new BufferedReader(mountsReader)) {
          String fileSystem;
          while ((fileSystem = bufferedMountsReader.readLine()) != null) {
            // file system type \s+ mount point \s+ etc.
            final String[] fsParts = fileSystem.split("\\s+");
            if (fsParts.length == 0) {
              continue;
            }

            final String fsType = fsParts[2];
            // all cgroup controllers have "cgroup" as file system type
            if (fsType.equals("cgroup")) {
              // get mounting path of cgroup
              final String fsMountingPath = fsParts[1];
              final String[] fsPathParts = fsMountingPath.split(File.separator);
              if (fsPathParts[fsPathParts.length - 1].equals("memory")) {
                memoryCGroupRoot = fsMountingPath;
              }
            }
          }
        } catch (final IOException e) {
          logger.errorNoDb("Error during reading a list of mounted file systems", e);
          memoryCGroupRoot = DEFAULT_MEMORY_CGROUP_PATH;
        }

      } catch (final FileNotFoundException fnfe) {
        memoryCGroupRoot = DEFAULT_MEMORY_CGROUP_PATH;
      }
    }

    if (memoryCGroupRoot == null) {
      memoryCGroupRoot = DEFAULT_MEMORY_CGROUP_PATH;
    }

    return memoryCGroupRoot;
  }

  /**
   * Obtains the total size in bytes of the installed physical memory on this machine. Note that on
   * some VMs it's impossible to obtain the physical memory size, in this case the return value will
   * {@code -1}.
   *
   * @return the total physical memory size in bytes or {@code <= 0} if the size can't be obtained.
   */
  private long getPhysicalMemorySize() {
    long osMemory = -1;

    try {
      final MBeanServer mBeanServer = ManagementFactory.getPlatformMBeanServer();
      final Object attribute =
          mBeanServer.getAttribute(
              new ObjectName("java.lang", "type", "OperatingSystem"), "TotalPhysicalMemorySize");

      if (attribute != null) {
        if (attribute instanceof Long) {
          osMemory = (Long) attribute;
        } else {
          try {
            osMemory = Long.parseLong(attribute.toString());
          } catch (final NumberFormatException e) {
            if (!logger.isDebugEnabled())
              logger.warnNoDb("Unable to determine the amount of installed RAM.");
            else logger.debugNoDb("Unable to determine the amount of installed RAM.", e);
          }
        }
      } else {
        if (!logger.isDebugEnabled())
          logger.warnNoDb("Unable to determine the amount of installed RAM.");
      }
    } catch (MalformedObjectNameException
        | AttributeNotFoundException
        | InstanceNotFoundException
        | MBeanException
        | ReflectionException e) {
      if (!logger.isDebugEnabled())
        logger.warnNoDb("Unable to determine the amount of installed RAM.");
      else logger.debugNoDb("Unable to determine the amount of installed RAM.", e);
    } catch (final RuntimeException e) {
      logger.warnNoDb("Unable to determine the amount of installed RAM.", e);
    }

    return osMemory;
  }

  private static long convertToMB(final long bytes) {
    if (bytes < 0) return bytes;

    return bytes / (1024 * 1024);
  }

  private static long convertToGB(final long bytes) {
    if (bytes < 0) return bytes;

    return bytes / (1024 * 1024 * 1024);
  }

  public static final class MemoryLimitResult {
    public final long memoryLimit;
    public final boolean insideContainer;

    MemoryLimitResult(final long memoryLimit, final boolean insideContainer) {
      this.memoryLimit = memoryLimit;
      this.insideContainer = insideContainer;
    }
  }

  public long blockSize(String path) {
    // TODO:When upgrading to java 11 use FileStore.getBlockSize()
    try (RandomAccessFile file = new RandomAccessFile(path, "r")) {
      return posix.fstat(file.getFD()).blockSize();
    } catch (Exception e) {
      logger.warn("Error detecting block size ignoring", e);
      return 4096;
    }
  }

  public boolean isOsRoot() {
    return OIOUtils.isOsLinux() && posix.getegid() == 0;
  }
}
