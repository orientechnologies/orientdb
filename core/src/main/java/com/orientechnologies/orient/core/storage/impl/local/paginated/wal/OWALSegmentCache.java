/*
 *
 *  *  Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
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
package com.orientechnologies.orient.core.storage.impl.local.paginated.wal;

import com.orientechnologies.common.log.OLogManager;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.*;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import static com.orientechnologies.common.io.OIOUtils.readByteBuffer;
import static com.orientechnologies.common.io.OIOUtils.writeByteBuffers;

/**
 * Write cache for the file which presents segment of WAL.
 * <p>
 * The last written pages will be cached so later they can be flushed by single batch.
 * File will be closed if there will be no access to this cache during last N min.
 */
public class OWALSegmentCache {
  /**
   * File is closed automatically in background thread.
   * This is timeout after which if we can not shutdown thread during cache close we treat it as exceptional situation.
   *
   * @see #fileTTL
   * @see FileCloser
   */
  private static final int CLOSER_TIMEOUT_MIN = 15;

  /**
   * Cache wide mutex
   */
  private final Object lockObject = new Object();

  /**
   * Path to the underlying file of WAL segment
   */
  private final Path path;

  private FileChannel segChannel;

  /**
   * Index of first page contained in cache.
   * Pages are cached continuously.
   */
  private       long             firstCachedPage = -1;
  private final List<ByteBuffer> pageCache       = new ArrayList<>();

  /**
   * Last time when cache was accessed in ns.
   */
  private long lastAccessTime = -1;

  /**
   * The maximum limit time limit after which if there were no accesses to the file , it will be closed automatically.
   */
  private final int fileTTL;
  private final int bufferSize;

  private long lastWrittenPageIndex = -1;

  /**
   * Content of last page written in file
   */
  private ByteBuffer lastWrittenPage;

  private final ScheduledExecutorService closer;

  OWALSegmentCache(final Path path, int fileTTL, int bufferSize, ScheduledExecutorService closer) {
    this.path = path;

    this.fileTTL = fileTTL;
    this.bufferSize = bufferSize;
    this.closer = closer;
  }

  /**
   * Writes page with given page index to the cache and eventually writes it to the file.
   */
  void writePage(ByteBuffer page, long pageIndex) throws IOException {
    synchronized (lockObject) {
      lastAccessTime = System.nanoTime();

      if (pageIndex >= firstCachedPage && pageIndex <= firstCachedPage + pageCache.size()) {
        if (pageIndex < firstCachedPage + pageCache.size()) {
          pageCache.set((int) (pageIndex - firstCachedPage), page);
        } else {
          pageCache.add(page);
        }
      } else if (pageCache.isEmpty()) {
        pageCache.add(page);
        firstCachedPage = pageIndex;
      }

      lastWrittenPage = page;
      lastWrittenPageIndex = pageIndex;

      if (pageCache.size() * OWALPage.PAGE_SIZE >= bufferSize + OWALPage.PAGE_SIZE) {
        flushAllBufferPagesExceptLastOne();
      }
    }
  }

  private void flushAllBufferPagesExceptLastOne() throws IOException {
    if (pageCache.size() > 1) {
      final ByteBuffer[] buffers = pageCache.toArray(new ByteBuffer[0]);
      final ByteBuffer[] buffersToFlush = new ByteBuffer[buffers.length - 1];

      for (int i = 0; i < buffersToFlush.length; i++) {
        buffersToFlush[i] = buffers[i];
        buffersToFlush[i].position(0);
      }

      initFile();

      segChannel.position(firstCachedPage * OWALPage.PAGE_SIZE);
      writeByteBuffers(buffersToFlush, segChannel, OWALPage.PAGE_SIZE * buffersToFlush.length);

      pageCache.clear();
      pageCache.add(buffers[buffers.length - 1]);

      firstCachedPage += buffersToFlush.length;
    }
  }

  private void flushBuffer() throws IOException {
    if (!pageCache.isEmpty()) {
      final ByteBuffer[] buffers = pageCache.toArray(new ByteBuffer[0]);

      for (ByteBuffer buffer : buffers) {
        buffer.position(0);
      }

      initFile();

      segChannel.position(firstCachedPage * OWALPage.PAGE_SIZE);
      writeByteBuffers(buffers, segChannel, OWALPage.PAGE_SIZE * buffers.length);

      pageCache.clear();
      firstCachedPage = -1;
    }
  }

  /**
   * Read page content with given index from cache or file.
   */
  byte[] readPage(long pageIndex) throws IOException {
    synchronized (lockObject) {
      lastAccessTime = System.nanoTime();

      if (pageIndex == lastWrittenPageIndex) {
        return lastWrittenPage.array();
      }

      if (pageIndex >= firstCachedPage && pageIndex < firstCachedPage + pageCache.size()) {
        final ByteBuffer buffer = pageCache.get((int) (pageIndex - firstCachedPage));
        return buffer.array();
      }

      final ByteBuffer buffer = ByteBuffer.allocate(OWALPage.PAGE_SIZE).order(ByteOrder.nativeOrder());

      initFile();
      segChannel.position(pageIndex * OWALPage.PAGE_SIZE);
      readByteBuffer(buffer, segChannel);

      return buffer.array();
    }
  }

  /**
   * Flushes all buffered pages and truncates file till passed in page index
   */
  void truncate(long pageIndex) throws IOException {
    synchronized (lockObject) {
      lastAccessTime = System.nanoTime();

      flushBuffer();

      lastWrittenPageIndex = -1;
      lastWrittenPage = null;

      segChannel.truncate(pageIndex * OWALPage.PAGE_SIZE);
    }
  }

  /**
   * Reads page content from the cache to the <code>ByteBuffer</code>
   * <code>ByteBuffer</code> is not backed by the cache and can be freely changed
   */
  ByteBuffer readPageBuffer(long pageIndex) throws IOException {
    synchronized (lockObject) {
      lastAccessTime = System.nanoTime();

      if (pageIndex == lastWrittenPageIndex) {
        final ByteBuffer copy = ByteBuffer.allocate(OWALPage.PAGE_SIZE).order(ByteOrder.nativeOrder());

        lastWrittenPage.position(0);
        copy.put(lastWrittenPage);

        return copy;
      }

      if (pageIndex >= firstCachedPage && pageIndex < firstCachedPage + pageCache.size()) {
        final ByteBuffer buffer = pageCache.get((int) (pageIndex - firstCachedPage));
        final ByteBuffer copy = ByteBuffer.allocate(OWALPage.PAGE_SIZE).order(ByteOrder.nativeOrder());

        buffer.position(0);
        copy.put(buffer);

        return copy;
      }

      final ByteBuffer buffer = ByteBuffer.allocate(OWALPage.PAGE_SIZE).order(ByteOrder.nativeOrder());

      initFile();

      segChannel.position(pageIndex * OWALPage.PAGE_SIZE);
      readByteBuffer(buffer, segChannel);

      buffer.position(0);

      return buffer;
    }
  }

  /**
   * Writes cache content to the file and performs <code>fsync</code>
   */
  public void sync() throws IOException {
    synchronized (lockObject) {
      if (segChannel != null) {
        lastAccessTime = System.nanoTime();
        flushBuffer();

        segChannel.force(false);
      }
    }
  }

  /**
   * Writes cache content to the file and closes it.
   * Calls <code>fsync</code> if needed.
   */
  public void close(boolean flush) {

    closer.shutdown();
    try {
      if (!closer.awaitTermination(CLOSER_TIMEOUT_MIN, TimeUnit.MINUTES)) {
        OLogManager.instance().error(this, "Can not close file " + path.getFileName(), null);
      } else {
        synchronized (lockObject) {
          try {
            if (segChannel != null) {
              closeFile(flush);
            }
          } catch (IOException ioe) {
            OLogManager.instance().error(this, "Can not close file " + path.getFileName(), ioe);
          }
        }
      }
    } catch (InterruptedException ie) {
      OLogManager.instance().warn(this, "WAL file " + path.getFileName() + " close was interrupted", ie);
    }

  }

  private void closeFile(boolean flush) throws IOException {
    if (flush) {
      flushBuffer();

      segChannel.force(false);
    }

    segChannel.close();
    segChannel = null;
  }

  /**
   * Clears cache and deletes underlying file
   */
  public void delete() throws IOException {

    closer.shutdown();

    try {
      if (!closer.awaitTermination(CLOSER_TIMEOUT_MIN, TimeUnit.MINUTES)) {
        OLogManager.instance().error(this, "Can not delete file " + path.getFileName(), null);
      } else {
        synchronized (lockObject) {
          try {
            if (segChannel != null) {
              segChannel.close();

              segChannel = null;
            }
          } catch (IOException ioe) {
            OLogManager.instance().error(this, "Can not delete file " + path.getFileName(), ioe);
          }

          Files.deleteIfExists(path);
        }
      }
    } catch (InterruptedException ie) {
      OLogManager.instance().warn(this, "WAL file " + path.getFileName() + " delete was interrupted", ie);
    }

  }

  /**
   * Initializes cache and opens underlying file.
   */
  public void open() throws IOException {
    synchronized (lockObject) {
      lastAccessTime = System.nanoTime();
      initFile();

      long pagesCount = segChannel.size() / OWALPage.PAGE_SIZE;

      if (segChannel.size() % OWALPage.PAGE_SIZE > 0) {
        OLogManager.instance().error(this, "Last WAL page was written partially, auto fix", null);

        segChannel.truncate(OWALPage.PAGE_SIZE * pagesCount);
      }

      firstCachedPage = -1;
      pageCache.clear();

      lastWrittenPage = null;
      lastWrittenPageIndex = -1;
    }
  }

  /**
   * @return Size of file in the amount of pages
   */
  public long filledUpTo() throws IOException {
    synchronized (lockObject) {
      lastAccessTime = System.nanoTime();
      initFile();

      final long endPage = segChannel.size() / OWALPage.PAGE_SIZE;
      if (firstCachedPage == -1)
        return endPage;

      return Math.max(endPage, firstCachedPage + pageCache.size());
    }
  }

  private void initFile() throws IOException {
    if (segChannel == null) {
      segChannel = FileChannel.open(path, StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE);

      if (fileTTL > 0) {
        FileCloser task = new FileCloser();
        task.self = closer.scheduleWithFixedDelay(task, fileTTL, fileTTL, TimeUnit.SECONDS);
      }
    }
  }

  private final class FileCloser implements Runnable {
    /**
     * Link to itself. It is needed to stop scheduled execution of task if file is already closed.
     */
    private volatile ScheduledFuture<?> self = null;

    @Override
    public void run() {
      synchronized (lockObject) {
        if (segChannel == null) {
          if (self != null) {
            self.cancel(false);
          }
        } else {
          long now = System.nanoTime();

          if (lastAccessTime == -1 || TimeUnit.SECONDS.convert(now - lastAccessTime, TimeUnit.NANOSECONDS) > fileTTL) {
            try {
              closeFile(true);

              if (self != null) {
                self.cancel(false);
              }

            } catch (IOException ioe) {
              OLogManager.instance().error(this, "Can not auto close file in WAL", ioe);
            }
          }
        }
      }
    }
  }
}
