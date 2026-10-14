package com.orientechnologies.orient.distributed.db;

import com.orientechnologies.common.concur.OTimeoutException;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.log.OLogger;
import com.orientechnologies.orient.distributed.context.coordination.sync.OSyncState;
import java.io.IOException;
import java.io.InputStream;
import java.io.InterruptedIOException;
import java.nio.channels.InterruptedByTimeoutException;
import java.util.Comparator;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class OReceiverInputStream extends InputStream {
  private static final OLogger logger = OLogManager.instance().logger(OReceiverInputStream.class);

  private record Buffer(byte[] content, long sequential, boolean last) {}

  public interface RequestNext {
    void requestNext(OSyncState state, boolean b);
  }

  private volatile Buffer buffer = new Buffer(new byte[] {}, -1, false);
  private volatile int cursor = 0;
  private final PriorityBlockingQueue<Buffer> buffers =
      new PriorityBlockingQueue<>(100, Comparator.comparing(Buffer::sequential));
  private final RequestNext ctx;
  private final OSyncState state;
  private volatile boolean localClose = false;

  private final Lock readMonitor = new ReentrantLock();
  private final Condition condition = readMonitor.newCondition();

  public OReceiverInputStream(RequestNext ctx, OSyncState state) {
    this.ctx = ctx;
    this.state = state;
  }

  @Override
  public int read() throws IOException {
    // TODO: impl also optimized int read(byte[] b, int off, int len)
    readMonitor.lock();
    if (localClose) {
      logger.debug("Local closed stream");
      return -1;
    }
    try {
      while (cursor == buffer.content().length) {
        if (buffer.last()) {
          logger.debug("Sender send last message");
          this.state.close();
          return -1;
        }
        try {
          Buffer bff = buffers.peek();
          if (bff != null && buffer.sequential() + 1 == bff.sequential()) {
            bff = buffers.poll();
            if (!bff.last()) {
              ctx.requestNext(state, false);
            }
            buffer = bff;
            cursor = 0;
          } else if (bff != null && bff.sequential() <= buffer.sequential()) {
            // Duplicate queued message drop
            buffers.poll();
          } else if (!condition.await(5, TimeUnit.MINUTES)) {
            // Failed to wait for a buffer, interrupt
            throw new InterruptedByTimeoutException();
          }

        } catch (InterruptedException e) {
          var ex = new InterruptedIOException();
          ex.initCause(e);
          throw ex;
        }
      }
      byte b = buffer.content()[cursor];
      cursor++;
      return b & 0xFF;
    } finally {
      readMonitor.unlock();
    }
  }

  @Override
  public int available() throws IOException {
    return buffer.content().length - cursor;
  }

  public void receive(byte[] buffer, long sequential, boolean last) {
    var received = new Buffer(buffer, sequential, last);
    var offered = this.buffers.offer(received, 5, TimeUnit.MINUTES);
    if (!offered) {
      throw new OTimeoutException("Timeout waiting for sync data");
    }
    readMonitor.lock();
    try {
      condition.signal();
    } finally {
      readMonitor.unlock();
    }
  }

  @Override
  public void close() throws IOException {
    if (!localClose) {
      // Send a close to the sender
      ctx.requestNext(state, true);
      // Queue a close state
      localClose = true;
    }
  }
}
