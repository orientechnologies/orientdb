package com.orientechnologies.orient.distributed.db;

import com.orientechnologies.orient.distributed.context.OSyncState;
import java.io.IOException;
import java.io.OutputStream;

public class OutputStreamMessages extends OutputStream {

  private OSyncState state;
  private OrientDBDistributed ctx;

  public OutputStreamMessages(OrientDBDistributed ctx, OSyncState state) {
    this.state = state;
    this.ctx = ctx;
  }

  @Override
  public void write(int b) throws IOException {
    write(new byte[] {(byte) b}, 0, 1);
  }

  @Override
  public void write(byte[] b, int off, int len) throws IOException {
    byte[] data = new byte[len];
    System.arraycopy(b, off, data, 0, len);
    this.ctx.sendBuffer(state, data);
  }

  @Override
  public void close() throws IOException {
    this.ctx.finishSync(state);
  }
}
