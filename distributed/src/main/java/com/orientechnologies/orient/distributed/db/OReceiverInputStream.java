package com.orientechnologies.orient.distributed.db;

import com.orientechnologies.common.concur.lock.OInterruptedException;
import com.orientechnologies.common.exception.OException;
import com.orientechnologies.orient.distributed.context.OSyncState;
import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

public class OReceiverInputStream extends InputStream {

  public interface RequestNext {
    void requestNext(OSyncState state, boolean b);
  }

  private byte[] buffer = new byte[] {};
  private int cursor = 0;
  private final BlockingQueue<byte[]> buffers = new ArrayBlockingQueue<byte[]>(3);
  private final RequestNext ctx;
  private final OSyncState state;
  private final AtomicBoolean finished = new AtomicBoolean(false);

  public OReceiverInputStream(RequestNext ctx, OSyncState state) {
    this.ctx = ctx;
    this.state = state;
  }

  @Override
  public int read() throws IOException {
    // TODO: impl also optimized int read(byte[] b, int off, int len)
    while (cursor == buffer.length) {
      if (finished.get()) {
        return -1;
      }
      try {
        buffer = buffers.take();
        cursor = 0;
        ctx.requestNext(state, false);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw OException.wrapException(new OInterruptedException("Receive sync interrupted"), e);
      }
    }
    byte b = buffer[cursor];
    cursor++;
    return b & 0xFF;
  }

  @Override
  public int available() throws IOException {
    return buffer.length - cursor;
  }

  public void receive(byte[] buffer, boolean finished) {
    this.buffers.add(buffer);
    if (finished) {
      this.finished.set(finished);
    }
  }

  @Override
  public void close() throws IOException {
    ctx.requestNext(state, true);
  }
}
