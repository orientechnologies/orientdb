package com.orientechnologies.orient.distributed.db;

import com.orientechnologies.common.concur.OTimeoutException;
import com.orientechnologies.common.concur.lock.OInterruptedException;
import com.orientechnologies.common.exception.OException;
import com.orientechnologies.orient.distributed.context.coordination.sync.OSyncState;
import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

public class OReceiverInputStream extends InputStream {

  private record Buffer(byte[] content, boolean finished) {}

  public interface RequestNext {
    void requestNext(OSyncState state, boolean b);
  }

  private byte[] buffer = new byte[] {};
  private int cursor = 0;
  private final BlockingQueue<Buffer> buffers = new ArrayBlockingQueue<Buffer>(10);
  private final RequestNext ctx;
  private final OSyncState state;
  private volatile boolean finished = false;

  public OReceiverInputStream(RequestNext ctx, OSyncState state) {
    this.ctx = ctx;
    this.state = state;
  }

  @Override
  public int read() throws IOException {
    // TODO: impl also optimized int read(byte[] b, int off, int len)
    while (cursor == buffer.length) {
      if (finished) {
        return -1;
      }
      try {
        Buffer bi = buffers.poll(5, TimeUnit.MINUTES);
        if (bi == null) {
          throw new OTimeoutException("Timeout waiting for sync data");
        }
        buffer = bi.content;
        cursor = 0;
        if (bi.finished) {
          this.finished = true;
          this.state.close();
        } else {
          ctx.requestNext(state, false);
        }
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
    try {
      var offered = this.buffers.offer(new Buffer(buffer, finished), 5, TimeUnit.MINUTES);
      if (!offered) {
        throw new OTimeoutException("Timeout waiting for sync data");
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw OException.wrapException(new OInterruptedException("Receive sync interrupted"), e);
    }
  }

  @Override
  public void close() throws IOException {
    if (!finished) {
      ctx.requestNext(state, true);
      // Close the state
      receive(new byte[] {}, true);
    }
  }
}
