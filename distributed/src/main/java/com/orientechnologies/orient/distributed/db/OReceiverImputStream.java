package com.orientechnologies.orient.distributed.db;

import com.orientechnologies.common.concur.lock.OInterruptedException;
import com.orientechnologies.common.exception.OException;
import com.orientechnologies.orient.distributed.context.OSyncState;
import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

public class OReceiverImputStream extends InputStream {

  private byte[] buffer;
  private int cursor;
  private BlockingQueue<byte[]> buffers = new ArrayBlockingQueue<byte[]>(3);
  private OrientDBDistributed ctx;
  private OSyncState state;

  public OReceiverImputStream(OrientDBDistributed ctx, OSyncState state) {
    this.ctx = ctx;
    this.state = state;
  }

  @Override
  public int read() throws IOException {
    if (cursor == buffer.length) {
      try {
        buffer = buffers.take();
        cursor = 0;
        ctx.requestNext(state);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw OException.wrapException(new OInterruptedException("Receive sync interrupted"), e);
      }
    }
    byte b = buffer[cursor];
    cursor++;
    return b;
  }

  public void receive(byte[] buffer) {
    this.buffers.add(buffer);
  }
}
