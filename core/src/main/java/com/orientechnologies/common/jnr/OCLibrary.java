package com.orientechnologies.common.jnr;

public interface OCLibrary {
  /** Address space limit. */
  int RLIMIT_AS = 9;

  int RLIMIT_NOFILE = 7;

  int getpagesize() throws LastErrorException;

  int pathconf(String path, int name) throws LastErrorException;
}
