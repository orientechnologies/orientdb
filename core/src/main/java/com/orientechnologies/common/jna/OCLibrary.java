package com.orientechnologies.common.jna;

import com.sun.jna.Library;
import com.sun.jna.Structure;
import com.sun.jna.platform.unix.Resource;

import java.util.List;

public interface OCLibrary extends Library {
  /**
   * Address space limit.
   */
  int RLIMIT_AS = 9;

  /**
   * Denotes no limit on a resource.
   */
  int RLIM_INFINITY = 0;

  class Rlimit extends Structure {
    public static final List<String> FIELDS = createFieldsOrder("rlim_cur", "rlim_max");

    /**
     * The current (soft) limit.
     */
    public long rlim_cur;

    /**
     * The hard limit.
     */
    public long rlim_max;

    @Override
    protected List<String> getFieldOrder() {
      return FIELDS;
    }
  }

  long getpid();

  // see man(2) rlimit
  int getrlimit(int resource, Rlimit rlim);
}
