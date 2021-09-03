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
public class OHttpMultipartBaseInputStream extends InputStream {

  protected ArrayList<Integer> buffer;
  protected int contentLength = 0;
  protected InputStream wrappedInputStream;

  public OHttpMultipartBaseInputStream(
      final InputStream in, final int iSkipInput, final int iContentLength) {
    wrappedInputStream = in;
    contentLength = iContentLength;
    this.buffer = new ArrayList<Integer>();
    this.buffer.add(iSkipInput);
  }

  public InputStream getWrappedInputStream() {
    return wrappedInputStream;
  }

  public void setSkipInput(final int iSkipInput) {
    this.buffer.add(iSkipInput);
    contentLength++;
  }

  public void setSkipInput(final ArrayList<Integer> iSkipInput) {
    this.buffer.addAll(iSkipInput);
    contentLength += iSkipInput.size();
  }

  @Override
  public synchronized int available() throws IOException {
    return contentLength;
  }

  @Override
  public synchronized int read() throws IOException {
    if (contentLength < 1) return -1;

    contentLength--;
    if (this.buffer.size() > 0) {
      final int returnValue = this.buffer.remove(0);
      return returnValue;
    }
    return wrappedInputStream.read();
  }

  @Override
  public void close() throws IOException {
    wrappedInputStream.close();
  }

  @Override
  public synchronized void mark(final int readlimit) {
    wrappedInputStream.mark(readlimit);
  }

  @Override
  public boolean markSupported() {
    return wrappedInputStream.markSupported();
  }

  @Override
  public int read(final byte[] b, final int off, final int len) throws IOException {
    if (buffer.size() > 0) {
      int tot2Read = Math.min(buffer.size(), len);

      for (int i = 0; i < tot2Read; ++i) {
        b[i] = (byte) buffer.remove(0).byteValue();
        contentLength--;
      }
      return tot2Read;
    }

    int totRead = wrappedInputStream.read(b, off, len);
    contentLength -= totRead;
    return totRead;
  }

  @Override
  public int read(byte[] b) throws IOException {
    return read(b, 0, b.length);
  }

  @Override
  public synchronized void reset() throws IOException {
    wrappedInputStream.reset();
  }

  @Override
  public long skip(final long n) throws IOException {
    return wrappedInputStream.skip(n);
  }

  @Override
  public String toString() {
    return wrappedInputStream.toString();
  }

  public void resetBuffer() {
    buffer.clear();
  }

  public int wrappedAvailable() throws IOException {
    return wrappedInputStream.available();
  }
}
