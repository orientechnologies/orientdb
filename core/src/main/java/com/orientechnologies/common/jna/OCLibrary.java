package com.orientechnologies.common.jna;

import com.sun.jna.LastErrorException;
import com.sun.jna.Library;
import com.sun.jna.NativeLong;
import com.sun.jna.Pointer;
import com.sun.jna.Structure;
import com.sun.jna.ptr.PointerByReference;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public interface OCLibrary extends Library {
  /**
   * Address space limit.
   */
  int RLIMIT_AS = 9;

  int RLIMIT_MEMLOCK = 8;

  int RLIMIT_NOFILE = 7;

  /**
   * Denotes no limit on a resource.
   */
  int RLIM_INFINITY = 0;

  class Rlimit extends Structure {
    public static final List<String> FIELDS = createFieldsOrder("rlimCur", "rlimMax");

    /**
     * The current (soft) limit.
     */
    public long rlimCur;

    /**
     * The hard limit.
     */
    public long rlimMax;

    @Override
    protected List<String> getFieldOrder() {
      return FIELDS;
    }

    /**
     * @param fields The structure field names in correct order
     *
     * @return An <U>un-modifiable</U> list of the fields
     */
    public static List<String> createFieldsOrder(String... fields) {
      return Collections.unmodifiableList(Arrays.asList(fields));
    }
  }

  long getpid();

  // see man(2) rlimit
  int getrlimit(int resource, Rlimit rlim);

  int open(String path, int flags, int mode) throws LastErrorException;

  int fallocate(int fd, int mode, long offset, long len) throws LastErrorException;

  long write(int fd, ByteBuffer buffer, long count) throws LastErrorException;

  long read(int fd, ByteBuffer buffer, long count) throws LastErrorException;

  int posix_memalign(PointerByReference memptr, NativeLong alignment, NativeLong size) throws LastErrorException;

  int fsync(int fd) throws LastErrorException;

  int close(int fd) throws LastErrorException;

  long lseek(int fd, long offset, int whence) throws LastErrorException;

  int getpagesize() throws LastErrorException;

  int pathconf(String path, int name) throws LastErrorException;

  int mlockall(int flags) throws LastErrorException;

  int mlock(Pointer pointer, long len) throws LastErrorException;

  int munlock(Pointer pointer, long len) throws LastErrorException;
}
