/*
 *
 * Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.orientechnologies.orient.server.network.protocol.http.multipart;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;

/** @author Luca Molino (molino.luca--at--gmail.com) */
public class OHttpMultipartContentInputStream extends InputStream {

  protected String boundary;

  protected OHttpMultipartBaseInputStream wrappedInputStream;

  protected int nextByte;

  protected boolean internalAvailable = true;

  public OHttpMultipartContentInputStream(OHttpMultipartBaseInputStream in, String iBoundary)
      throws IOException {
    wrappedInputStream = in;
    boundary = '\n' + "--" + iBoundary;
  }

  public InputStream getWrappedInputStream() {
    return wrappedInputStream;
  }

  @Override
  public synchronized int available() throws IOException {
    if (internalAvailable) {
      return wrappedInputStream.available();
    } else {
      return -1;
    }
  }

  @Override
  public synchronized int read() throws IOException {
    if (!internalAvailable) return -1;

    int value = nextByte;
    nextByte = wrappedInputStream.read();
    if (((char) nextByte) == '\r') {
      bufferData();
    }
    return value;
  }

  @Override
  public void close() throws IOException {}

  @Override
  public synchronized void mark(int readlimit) {
    wrappedInputStream.mark(readlimit);
  }

  @Override
  public boolean markSupported() {
    return wrappedInputStream.markSupported();
  }

  @Override
  public int read(byte[] b, int off, int len) throws IOException {
    return wrappedInputStream.read(b, off, len);
  }

  @Override
  public int read(byte[] b) throws IOException {
    return wrappedInputStream.read(b);
  }

  @Override
  public synchronized void reset() throws IOException {
    nextByte = wrappedInputStream.read();
    internalAvailable = true;
    if (((char) nextByte) == '\r') {
      bufferData();
    }
  }

  @Override
  public long skip(long n) throws IOException {
    return wrappedInputStream.skip(n);
  }

  @Override
  public String toString() {
    return wrappedInputStream.toString();
  }

  protected void bufferData() throws IOException {
    boolean checkingEnd = true;
    int boundaryCursor = 0;
    final ArrayList<Integer> buffer = new ArrayList<Integer>();
    int b;
    while (checkingEnd && (b = wrappedInputStream.read()) > -1) {
      buffer.add(b);
      if (((char) b) == boundary.charAt(boundaryCursor)) {
        internalAvailable = false;
        boundaryCursor++;
        if (boundaryCursor == boundary.length()) {
          checkingEnd = false;
          wrappedInputStream.resetBuffer();
        }
      } else {
        internalAvailable = true;
        checkingEnd = false;
        if (buffer.size() > 0) wrappedInputStream.setSkipInput(buffer);
      }
    }
  }
}
