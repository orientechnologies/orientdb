/*
 * Copyright 2010-2012 Luca Garulli (l.garulli--at--orientechnologies.com)
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
package com.orientechnologies.orient.core.storage.fs;

import java.lang.reflect.Method;
import java.nio.MappedByteBuffer;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.profiler.OProfilerMBean;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.memory.OMemoryWatchDog;

public class OMMapBufferEntry implements Comparable<OMMapBufferEntry> {
  private static final OProfilerMBean PROFILER = Orient.instance().getProfiler();
  private static final int          FORCE_DELAY;
  private static final int          FORCE_RETRY;

  static Method                     cleanerMethod;

  Lock                              lock     = new ReentrantLock();
  volatile OFileMMap                file;
  volatile MappedByteBuffer         buffer;
  final long                        beginOffset;
  final int                         size;
  volatile boolean                  dirty;
  private volatile long             lastUsed;

  static {
    FORCE_DELAY = OGlobalConfiguration.FILE_MMAP_FORCE_DELAY.getValueAsInteger();
    FORCE_RETRY = OGlobalConfiguration.FILE_MMAP_FORCE_RETRY.getValueAsInteger();

    // GET SUN JDK METHOD TO CLEAN MMAP BUFFERS
    try {
      final Class<?> sunClass = Class.forName("sun.nio.ch.DirectBuffer");
      cleanerMethod = sunClass.getMethod("cleaner");
    } catch (Exception e) {
      // IGNORE IT AND USE GC TO FREE RESOURCES
    }
  }

  public OMMapBufferEntry(final OFileMMap iFile, final MappedByteBuffer buffer, final long beginOffset, final int size) {
    this.file = iFile;
    this.buffer = buffer;
    this.beginOffset = beginOffset;
    this.size = size;
    this.dirty = false;
    updateLastUsedTime();
  }

  /**
   * Flushes the memory mapped buffer to disk only if it's dirty.
   * 
   * @return true if the buffer has been successfully flushed, otherwise false.
   */
  boolean flush() {
    lock.lock();
    try {
      if (!dirty)
        return true;

      final long timer = PROFILER.startChrono();

      // FORCE THE WRITE OF THE BUFFER
      for (int i = 0; i < FORCE_RETRY; ++i) {
        try {
          buffer.force();
          dirty = false;
          break;
        } catch (Exception e) {
          OLogManager.instance().debug(this,
              "Cannot write memory buffer to disk. Retrying (" + (i + 1) + "/" + FORCE_RETRY + ")...");
          OMemoryWatchDog.freeMemoryForResourceCleanup(FORCE_DELAY);
        }
      }

      if (dirty)
        OLogManager.instance().debug(this, "Cannot commit memory buffer to disk after %d retries", FORCE_RETRY);
      else
        PROFILER.updateCounter(PROFILER.getProcessMetric("file.mmap.pagesCommitted"), "Memory mapped pages committed to disk", +1);

      PROFILER.stopChrono(PROFILER.getProcessMetric("file.mmap.commitPages"), "Commit memory mapped pages to disk", timer);

      return !dirty;

    } finally {
      lock.unlock();
    }
  }

  @Override
  public String toString() {
    final StringBuilder builder = new StringBuilder();
    builder.append("OMMapBufferEntry [file=").append(file).append(", beginOffset=").append(beginOffset).append(", size=")
        .append(size).append("]");
    return builder.toString();
  }

  /**
   * Force closing of file if it's opened yet.
   */
  void close() {
    lock.lock();
    try {

      if (buffer != null) {
        if (dirty)
          buffer.force();

        if (cleanerMethod != null) {
          // USE SUN JVM SPECIAL METHOD TO FREE RESOURCES
          try {
            final Object cleaner = cleanerMethod.invoke(buffer);
            if (cleaner != null)
              cleaner.getClass().getMethod("clean").invoke(cleaner);
          } catch (Exception e) {
            OLogManager.instance().error(this, "Error on calling Sun's MMap buffer clean", e);
          }
        }

        buffer = null;
      }
      lastUsed = 0;
      file = null;

    } finally {
      lock.unlock();
    }
  }

  public int compareTo(final OMMapBufferEntry iOther) {
    return (int) (beginOffset - iOther.beginOffset);
  }

  boolean isValid() {
    return buffer != null;
  }

  boolean isDirty() {
    return dirty;
  }

  void setDirty() {
    this.dirty = true;
  }

  void acquireLock() {
    lock.lock();
  }

  void releaseLock() {
    lock.unlock();
  }

  public void updateLastUsedTime() {
    lastUsed = System.currentTimeMillis();
  }

  public long getLastUsed() {
    return lastUsed;
  }
}
