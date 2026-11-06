package com.orientechnologies.orient.server.distributed.impl.task;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;

public class FailsafeOutputStream extends OutputStream {

  private File file;
  private FileOutputStream out;

  public FailsafeOutputStream(File file) throws FileNotFoundException {
    this.file = file;
    this.out = new FileOutputStream(file);
  }

  @Override
  public void write(int b) throws IOException {
    if (out != null) {
      try {
        out.write(b);
      } catch (IOException e) {
        out.close();
        file.delete();
        out = null;
      }
    }
  }

  @Override
  public void write(byte[] b, int off, int len) throws IOException {
    if (out != null) {
      try {
        out.write(b, off, len);
      } catch (IOException e) {
        out.close();
        file.delete();
        out = null;
      }
    }
  }

  @Override
  public void flush() throws IOException {
    if (out != null) {
      try {
        out.flush();
      } catch (IOException e) {
        out.close();
        file.delete();
        out = null;
      }
    }
  }

  @Override
  public void close() throws IOException {
    if (out != null) {
      try {
        out.close();
      } catch (IOException e) {
        out.close();
        file.delete();
        out = null;
      }
    }
  }
}
