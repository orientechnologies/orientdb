package com.orientechnologies.common.jnr;

import jnr.ffi.NativeLong;
import jnr.ffi.byref.PointerByReference;

public interface OCLibrary {
  /** Address space limit. */
  int RLIMIT_AS = 9;

  int RLIMIT_MEMLOCK = 8;

  int RLIMIT_NOFILE = 7;

  /** Denotes no limit on a resource. */
  int RLIM_INFINITY = 0;

  int fallocate(int fd, int mode, long offset, long len) throws LastErrorException;

  int posix_memalign(PointerByReference memptr, NativeLong alignment, NativeLong size)
      throws LastErrorException;

  int getpagesize() throws LastErrorException;

  int pathconf(String path, int name) throws LastErrorException;
}
