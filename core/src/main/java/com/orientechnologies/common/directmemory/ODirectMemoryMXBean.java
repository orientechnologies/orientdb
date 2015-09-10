package com.orientechnologies.common.directmemory;

/**
 * @author Andrey Lomakin <lomakin.andrey@gmail.com>.
 * @since 9/8/2015
 */
public interface ODirectMemoryMXBean {
  long getSize();

  long getSizeInKB();

  long getSizeInMB();

  long getSizeInGB();
}
