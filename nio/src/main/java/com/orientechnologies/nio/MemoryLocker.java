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

package com.orientechnologies.nio;

import com.orientechnologies.common.log.OLogManager;
import com.sun.jna.Native;

/**
 * @author Artem Loginov (logart) logart2007@gmail.com Date: 5/29/12 Time: 11:31 PM
 */
public class MemoryLocker {
  private static void disableUsingSystemJNA() {
    if (System.getProperty("jna.nosys") == null || !System.getProperty("jna.nosys").equals("true")) {
      System.setProperty("jna.nosys", "true");
    }
  }

  public static void lockMemory(boolean useSystemJNADisabled) {
    if (useSystemJNADisabled)
      disableUsingSystemJNA();
    try {
      int errorCode = MemoryLockerLinux.INSTANCE.mlockall(MemoryLockerLinux.LOCK_CURRENT_MEMORY);
      if (errorCode != 0) {

        final String errorMessage;
        int lastError = Native.getLastError();
        switch (lastError) {
        case MemoryLockerLinux.EPERM:
          errorMessage = "The calling process does not have the appropriate privilege to perform the requested operation(EPERM).";
          break;
        case MemoryLockerLinux.EAGAIN:
          errorMessage = "Some or all of the memory identified by the operation could not be locked when the call was made(EAGAIN).";
          break;
        case MemoryLockerLinux.ENOMEM:
          errorMessage = "Unable to lock JVM memory. This can result in part of the JVM being swapped out, especially if mmapping of files enabled. Increase RLIMIT_MEMLOCK or run OrientDB server as root(ENOMEM).";
          break;
        case MemoryLockerLinux.EINVAL:
          errorMessage = "The flags argument is zero, or includes unimplemented flags(EINVAL).";
          break;
        case MemoryLockerLinux.ENOSYS:
          errorMessage = "The implementation does not support this memory locking interface(ENOSYS).";
          break;
        default:
          errorMessage = "Unexpected exception with code " + lastError + ".";
          break;
        }
        OLogManager.instance().error(null, "[MemoryLocker.lockMemory] Error occurred while locking memory!\n" + errorMessage);

      } else {
        OLogManager.instance().info(null, "[MemoryLocker.lockMemory] Memory locked successfully!");
      }

    } catch (UnsatisfiedLinkError e) {
      OLogManager
          .instance()
          .warn(null,
              "[MemoryLocker.lockMemory] Error on locking memory! It seems that you operation system doesn't support native mlockall call!");
    }
  }
}
