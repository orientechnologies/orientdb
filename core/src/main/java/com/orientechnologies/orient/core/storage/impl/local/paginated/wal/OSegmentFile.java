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
import com.orientechnologies.orient.core.storage.OStorageAbstract;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

public class OSegmentFile {
  private final Object lockObject = new Object();

  private final File file;

  private RandomAccessFile segFile;
  private FileChannel      segChannel;

  private long             firstCachedPage = -1;
  private List<ByteBuffer> pageCache       = new ArrayList<ByteBuffer>();

  private long lastAccessTime = -1;

  private final int fileTTL;
  private final int bufferSize;

  private long lastWrittenPageIndex = -1;
  private ByteBuffer lastWrittenPage;

  private ScheduledExecutorService closer;

  public OSegmentFile(final File file, int fileTTL, int bufferSize) {
    this.file = file;
    this.fileTTL = fileTTL;
    this.bufferSize = bufferSize;

    closer = Executors.newScheduledThreadPool(1, new ThreadFactory() {
      @Override
      public Thread newThread(Runnable r) {
        final Thread thread = new Thread(OStorageAbstract.storageThreadGroup, r);
        thread.setDaemon(true);
        thread.setName("Closer for WAL file " + file.getName());
        return thread;
      }
    });
  }

  public void writePage(ByteBuffer page, long pageIndex) throws IOException {
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

  public byte[] readPage(long pageIndex) throws IOException {
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

  public ByteBuffer readPageBuffer(long pageIndex) throws IOException {
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

  public void sync() throws IOException {
    synchronized (lockObject) {
      if (segChannel != null) {
        lastAccessTime = System.nanoTime();
        flushBuffer();

        segChannel.force(false);
      }
    }
  }

  public void close(boolean flush) {

    closer.shutdown();
    try {
      if (!closer.awaitTermination(15, TimeUnit.MINUTES)) {
        OLogManager.instance().error(this, "Can not close file " + file.getName());
      } else {
        synchronized (lockObject) {
          try {
            if (segFile != null) {
              closeFile(flush);
            }
          } catch (IOException ioe) {
            OLogManager.instance().error(this, "Can not close file " + file.getName(), ioe);
          }
        }
      }
    } catch (InterruptedException ie) {
      OLogManager.instance().warn(this, "WAL file " + file.getName() + " close was interrupted", ie);
    }

  }

  private void closeFile(boolean flush) throws IOException {
    if (flush) {
      flushBuffer();

      segChannel.force(false);
    }

    segFile.close();

    segFile = null;
    segChannel = null;
  }

  public void delete() {

    closer.shutdown();

    try {
      if (!closer.awaitTermination(15, TimeUnit.MINUTES)) {
        OLogManager.instance().error(this, "Can not delete file " + file.getName());
      } else {
        synchronized (lockObject) {
          try {
            segFile.close();

            segFile = null;
            segChannel = null;
          } catch (IOException ioe) {
            OLogManager.instance().error(this, "Can not delete file " + file.getName(), ioe);
          }

          if (!file.delete()) {
            OLogManager.instance().error(this, "Can not delete file " + file.getName());
          }
        }
      }
    } catch (InterruptedException ie) {
      OLogManager.instance().warn(this, "WAL file " + file.getName() + " delete was interrupted", ie);
    }

  }

  public void open() throws IOException {
    synchronized (lockObject) {
      lastAccessTime = System.nanoTime();
      initFile();

      long pagesCount = segFile.length() / OWALPage.PAGE_SIZE;

      if (segFile.length() % OWALPage.PAGE_SIZE > 0) {
        OLogManager.instance().error(this, "Last WAL page was written partially, auto fix");

        segFile.setLength(OWALPage.PAGE_SIZE * pagesCount);
      }

      firstCachedPage = -1;
      pageCache.clear();

      lastWrittenPage = null;
      lastWrittenPageIndex = -1;
    }
  }

  public long filledUpTo() throws IOException {
    synchronized (lockObject) {
      lastAccessTime = System.nanoTime();
      initFile();

      final long endPage = segFile.length() / OWALPage.PAGE_SIZE;
      if (firstCachedPage == -1)
        return endPage;

      return Math.max(endPage, firstCachedPage + pageCache.size());
    }
  }

  private void initFile() throws IOException {
    if (segFile == null) {
      segFile = new RandomAccessFile(file, "rw");
      segChannel = segFile.getChannel();

      if (fileTTL > 0) {
        FileCloser task = new FileCloser();
        task.self = closer.scheduleWithFixedDelay(task, fileTTL, fileTTL, TimeUnit.SECONDS);
      }
    }
  }

  private void writeByteBuffers(ByteBuffer[] buffers, FileChannel channel, long bytesToWrite) throws IOException {
    long written = 0;

    for (ByteBuffer buffer : buffers) {
      buffer.position(0);
    }

    while (written < bytesToWrite) {
      final int bufferIndex = (int) written / OWALPage.PAGE_SIZE;
      final int bufferOffset = (int) (written - OWALPage.PAGE_SIZE * bufferIndex);

      if (bufferOffset > 0) {
        ByteBuffer buffer = buffers[bufferIndex];
        buffer.position(bufferOffset);
      }

      written += channel.write(buffers, bufferIndex, buffers.length - bufferIndex);
    }
  }

  private void readByteBuffer(ByteBuffer buffer, FileChannel channel) throws IOException {
    int bytesToRead = buffer.limit();

    int read = 0;
    while (read < bytesToRead) {
      buffer.position(read);

      final int r = channel.read(buffer);
      if (r < 0)
        throw new IllegalStateException("End of file " + file + " is reached");

      read += r;
    }
  }

  private final class FileCloser implements Runnable {
    private volatile ScheduledFuture<?> self = null;

    @Override
    public void run() {
      synchronized (lockObject) {
        if (segFile == null) {
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
