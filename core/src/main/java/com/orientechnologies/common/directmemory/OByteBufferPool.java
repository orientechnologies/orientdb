package com.orientechnologies.common.directmemory;

import com.orientechnologies.orient.core.config.OGlobalConfiguration;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.concurrent.ConcurrentLinkedQueue;

public class OByteBufferPool {
  private static final OByteBufferPool INSTANCE = new OByteBufferPool(
      OGlobalConfiguration.DISK_CACHE_PAGE_SIZE.getValueAsInteger() * 1024);
  private final int        pageSize;
  private final ByteBuffer zeroPage;

  private final ConcurrentLinkedQueue<ByteBuffer> pool = new ConcurrentLinkedQueue<ByteBuffer>();

  public static OByteBufferPool instance() {
    return INSTANCE;
  }

  public OByteBufferPool(int pageSize) {
    this.pageSize = pageSize;
    this.zeroPage = ByteBuffer.allocateDirect(pageSize).order(ByteOrder.nativeOrder());
  }

  public ByteBuffer acquireDirect(boolean clear) {
    final ByteBuffer buffer = pool.poll();

    if (buffer != null) {
      buffer.position(0);

      if (clear) {
        buffer.put(zeroPage.duplicate());
        buffer.position(0);
      }

      return buffer;
    }

    return ByteBuffer.allocateDirect(pageSize).order(ByteOrder.nativeOrder());
  }

  public void release(ByteBuffer buffer) {
    pool.offer(buffer);
  }
}
