package com.orientechnologies.orient.distributed.db;

import com.orientechnologies.common.concur.OTimeoutException;
import com.orientechnologies.common.concur.lock.OInterruptedException;
import com.orientechnologies.common.exception.OException;
import com.orientechnologies.orient.distributed.context.coordination.sync.OSyncState;
import java.io.IOException;
import java.io.InputStream;
import java.util.Comparator;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.TimeUnit;

public class OReceiverInputStream extends InputStream {

  private record Buffer(byte[] content, long sequential, boolean finished) {}

  public interface RequestNext {
    void requestNext(OSyncState state, boolean b);
  }

  private volatile byte[] buffer = new byte[] {};
  private volatile int cursor = 0;
  private final BlockingQueue<Buffer> buffers = new ArrayBlockingQueue<Buffer>(10);
  private final PriorityBlockingQueue<Buffer> outOfOrder =
      new PriorityBlockingQueue<>(10, Comparator.comparing(Buffer::sequential));
  private final RequestNext ctx;
  private final OSyncState state;
  private volatile boolean finished = false;
  private volatile long receiveSequential = -1;
  private volatile long readingSequential = -1;
  private final Object readMonitor = new Object();
  private final Object receiveMonitor = new Object();

  public OReceiverInputStream(RequestNext ctx, OSyncState state) {
    this.ctx = ctx;
    this.state = state;
  }

  @Override
  public int read() throws IOException {
    // TODO: impl also optimized int read(byte[] b, int off, int len)
    synchronized (readMonitor) {
      while (cursor == buffer.length) {
        if (finished) {
          return -1;
        }
        try {
          Buffer bi = null;
          if (!outOfOrder.isEmpty()) {
            if (readingSequential + 1 == outOfOrder.peek().sequential()) {
              bi = outOfOrder.poll();
            }
          }
          if (bi == null) {
            bi = buffers.poll(5, TimeUnit.MINUTES);
          }
          if (bi == null) {
            throw new OTimeoutException("Timeout waiting for sync data");
          }
          readingSequential = bi.sequential;
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
  }

  @Override
  public int available() throws IOException {
    return buffer.length - cursor;
  }

  public void receive(byte[] buffer, long sequential, boolean finished) {
    synchronized (receiveMonitor) {
      try {
        var received = new Buffer(buffer, sequential, finished);
        if (this.receiveSequential + 1 == received.sequential()) {
          this.receiveSequential = received.sequential();
          var offered = this.buffers.offer(received, 5, TimeUnit.MINUTES);
          if (!offered) {
            throw new OTimeoutException("Timeout waiting for sync data");
          }
        } else if (this.readingSequential + 1 > received.sequential()) {
          // ignore
        } else {
          outOfOrder.add(received);
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw OException.wrapException(new OInterruptedException("Receive sync interrupted"), e);
      }
    }
  }

  @Override
  public void close() throws IOException {
    if (!finished) {
      ctx.requestNext(state, true);
      // Close the state
      // TODO: check sequential
      receive(new byte[] {}, receiveSequential + 1, true);
    }
  }
}
