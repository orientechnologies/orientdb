package com.orientechnologies.common.directmemory;

import com.orientechnologies.orient.core.config.OGlobalConfiguration;

import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentLinkedQueue;

public class OByteBufferPool {
  private static final OByteBufferPool INSTANCE  = new OByteBufferPool();
  private static final int             PAGE_SIZE = OGlobalConfiguration.DISK_CACHE_PAGE_SIZE.getValueAsInteger() * 1024;

  private final ConcurrentLinkedQueue<ByteBuffer> pool = new ConcurrentLinkedQueue<ByteBuffer>();

  public static OByteBufferPool instance() {
    return INSTANCE;
  }

  public ByteBuffer acquireDirect(boolean clear) {
    final ByteBuffer buffer = pool.poll();

    if (buffer != null) {
      buffer.position(0);

      if (clear) {
        buffer.put(new byte[PAGE_SIZE]);
      }
      return buffer;
    }

    return ByteBuffer.allocateDirect(PAGE_SIZE);
  }

  public void release(ByteBuffer buffer) {
    pool.offer(buffer);
  }
}
