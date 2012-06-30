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

import com.sun.jna.Library;
import com.sun.jna.Native;

/**
 * @author Artem Loginov (logart) logart2007@gmail.com
 */
public interface MemoryLockerLinux extends Library {

  // http://www-numi.fnal.gov/offline_software/srt_public_context/WebDocs/Errors/unix_system_errors.html

  // #define EPERM 1 /* Operation not permitted */ The calling process does not have the appropriate privilege to perform the
  // requested operation.

  static final int  EPERM                                   = 1;

  // #define EAGAIN 11 /* Try again */ Some or all of the memory identified by the operation could not be locked when the call was
  // made.
  static final int  EAGAIN                                  = 11;

  // #define ENOMEM 12 /* Out of memory */ Locking all of the pages currently mapped into the address space of the process would
  // exceed an implementation-dependent limit on the amount of memory that the process may lock.
  static final int  ENOMEM                                  = 12;

  // #define EINVAL 22 /* Invalid argument */ The flags argument is zero, or includes unimplemented flags.
  static final int  EINVAL                                  = 22;

  // #define ENOSYS 38 /* Function not implemented */ The implementation does not support this memory locking interface.
  static final int  ENOSYS                                  = 38;

  // Linux/include/asm-generic/mman.h
  //
  // 16 #define MCL_CURRENT 1 /* lock all current mappings */
  // 17 #define MCL_FUTURE 2 /* lock all future mappings */

  static final int  LOCK_CURRENT_MEMORY                     = 1;
  static final int  LOCK_ALL_MEMORY_DURING_APPLICATION_LIFE = 2;

  MemoryLockerLinux INSTANCE                                = (MemoryLockerLinux) Native.loadLibrary("c", MemoryLockerLinux.class);

  /**
   * This method locks all memory under *nix operating system using kernel function {@code mlockall}. details of this function you
   * can find on {@see http://www.kernel.org/doc/man-pages/online/pages/man2/mlock.2.html}
   * 
   * @param flags
   *          determines lock memory on startup or during life of application.
   * 
   * @return Upon successful completion, the mlockall() function returns a value of zero. Otherwise, no additional memory is locked,
   *         and the function returns a value of -1 and sets errno to indicate the error. The effect of failure of mlockall() on
   *         previously existing locks in the address space is unspecified. If it is supported by the implementation, the
   *         munlockall() function always returns a value of zero. Otherwise, the function returns a value of -1 and sets errno to
   *         indicate the error.
   */
  int mlockall(int flags);
}
