package com.orientechnologies.orient.server.distributed.impl.task;

import java.io.IOException;
import java.io.OutputStream;

public class TeeOutputStream extends OutputStream {
  private OutputStream first;
  private OutputStream second;

  public TeeOutputStream(OutputStream first, OutputStream second) {
    this.first = first;
    this.second = second;
  }

  @Override
  public void write(int b) throws IOException {
    first.write(b);
    second.write(b);
  }

  @Override
  public void write(byte[] b) throws IOException {
    first.write(b);
    second.write(b);
  }

  @Override
  public void write(byte[] b, int off, int len) throws IOException {
    first.write(b, off, len);
    second.write(b, off, len);
  }

  @Override
  public void flush() throws IOException {
    first.flush();
    second.flush();
  }

  @Override
  public void close() throws IOException {
    first.close();
    second.close();
  }
}
