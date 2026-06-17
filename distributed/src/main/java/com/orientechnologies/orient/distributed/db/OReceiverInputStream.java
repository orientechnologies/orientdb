package com.orientechnologies.orient.distributed.db;

import com.orientechnologies.common.concur.OTimeoutException;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.log.OLogger;
import com.orientechnologies.orient.distributed.context.coordination.sync.OSyncState;
import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.InterruptedByTimeoutException;
import java.util.Comparator;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class OReceiverInputStream extends InputStream {
  private static final OLogger logger = OLogManager.instance().logger(OReceiverInputStream.class);

  private record Buffer(byte[] content, long sequential, boolean finished) {}

  public interface RequestNext {
    void requestNext(OSyncState state, boolean b);
  }

  private volatile byte[] buffer = new byte[] {};
  private volatile int cursor = 0;
  private final PriorityBlockingQueue<Buffer> buffers =
      new PriorityBlockingQueue<>(100, Comparator.comparing(Buffer::sequential));
  private final RequestNext ctx;
  private final OSyncState state;
  private volatile boolean finished = false;
  private volatile long readingSequential = -1;
  private volatile long receivingSequential = -1;
  private volatile boolean receivedClose = false;
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
    try {
      while (cursor == buffer.length) {
        if (finished) {
          this.state.close();
          return -1;
        }
        try {
          Buffer bi = buffers.peek();
          if (bi != null && readingSequential + 1 == bi.sequential()) {
            bi = buffers.poll();
            readingSequential = bi.sequential();
            buffer = bi.content;
            cursor = 0;
            if (bi.finished) {
              this.finished = true;
            } else {
              ctx.requestNext(state, false);
            }
          } else if (bi != null && bi.sequential() <= readingSequential) {
            // Duplicate queued message drop
            buffers.poll();
          } else if (!condition.await(5, TimeUnit.MINUTES)) {
            // Failed to wait for a buffer, interrupt
            throw new InterruptedByTimeoutException();
          }

        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new java.io.InterruptedIOException();
        }
      }
      byte b = buffer[cursor];
      cursor++;
      return b & 0xFF;
    } finally {
      readMonitor.unlock();
    }
  }

  @Override
  public int available() throws IOException {
    return buffer.length - cursor;
  }

  public void receive(byte[] buffer, long sequential, boolean finished) {
    var received = new Buffer(buffer, sequential, finished);
    if (receivingSequential < sequential) {
      receivingSequential = sequential;
    }
    if (finished) {
      receivedClose = true;
    }
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
    if (!receivedClose) {
      // Send a close to the sender
      ctx.requestNext(state, true);
      // Queue a close state
      receive(new byte[] {}, receivingSequential + 1, true);
    }
  }
}
