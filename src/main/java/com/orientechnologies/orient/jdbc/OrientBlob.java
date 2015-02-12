/**
 * Copyright 2014 Orient Technologies LTD (info(at)orientechnologies.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * 	http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * For more information: http://www.orientechnologies.com
 */
package com.orientechnologies.orient.jdbc;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.sql.Blob;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.ArrayList;
import java.util.List;

import com.orientechnologies.orient.core.record.impl.ORecordBytes;

import static java.util.Arrays.asList;

/**
 * @author Roberto Franchini (CELI Srl - franchini@celi.it)
 * @author Salvatore Piccione (TXT e-solutions SpA - salvo.picci@gmail.com)
 */
public class OrientBlob implements Blob {

  private final List<byte[]> binaryDataChunks;

  private long               length;

  private byte[]             currentChunk;

  private int                currentChunkIndex;

  protected OrientBlob(ORecordBytes binaryDataChunk) throws IllegalArgumentException, NullPointerException {
    this(asList(binaryDataChunk));
  }

  protected OrientBlob(List<ORecordBytes> binaryDataChunks) throws IllegalArgumentException, NullPointerException {
    this.binaryDataChunks = new ArrayList<byte[]>(binaryDataChunks.size());
    for (ORecordBytes binaryDataChunk : binaryDataChunks) {
      if (binaryDataChunk == null) {
        throw new IllegalArgumentException("The binary data chunks list cannot hold null chunks");
      } else if (binaryDataChunk.getSize() == 0) {
        throw new IllegalArgumentException("The binary data chunks list cannot hold empty chunks");
      } else {

        this.binaryDataChunks.add(binaryDataChunk.toStream());
      }
    }
    this.length = calculateLenght();
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.Blob#length()
   */
  public long length() throws SQLException {
    return this.length;
  }

  private long calculateLenght() {
    long length = 0;
    for (byte[] binaryDataChunk : binaryDataChunks) {
      length += binaryDataChunk.length;
    }
    return length;
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.Blob#getBytes(long, int)
   */
  public byte[] getBytes(long pos, int length) throws SQLException {
    if (pos < 1)
      throw new SQLException("The position of the first byte in the BLOB value to be " + "extracted cannot be less than 1");
    if (length < 0)
      throw new SQLException("The number of the consecutive bytes in the BLOB value to "
          + "be extracted cannot be a negative number");

    int relativeIndex = this.getRelativeIndex(pos);

    ByteBuffer buffer = ByteBuffer.allocate(length);
    int j;
    for (j = 0; j < length; j++) {
      if (relativeIndex == currentChunk.length) {
        // go to the next chunk, if any...
        currentChunkIndex++;
        if (currentChunkIndex < binaryDataChunks.size()) {
          // the next chunk exists so we update the relative index and
          // the current chunk reference
          relativeIndex = 0;
          currentChunk = binaryDataChunks.get(currentChunkIndex);
        } else
          // exit from the loop: there are no more bytes to be read
          break;
      }
      buffer.put(currentChunk[relativeIndex]);
      relativeIndex++;
    }

    return buffer.array();
  }

  /**
   * Calculates the index within a binary chunk corresponding to the given absolute position within this BLOB
   * 
   * @param pos
   * @return
   */
  private int getRelativeIndex(long pos) {
    int currentSize = 0;
    currentChunkIndex = 0;

    // loop until we find the chuks holding the given position
    while (pos >= (currentSize += binaryDataChunks.get(currentChunkIndex).length))
      currentChunkIndex++;

    currentChunk = binaryDataChunks.get(currentChunkIndex);
    currentSize -= currentChunk.length;
    // the position referred to the target binary chunk
    int relativePosition = (int) (pos - currentSize);
    // the index of the first byte to be returned
    return relativePosition - 1;
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.Blob#getBinaryStream()
   */
  public InputStream getBinaryStream() throws SQLException {
    return new OrientBlobInputStream();
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.Blob#position(byte[], long)
   */
  public long position(byte[] pattern, long start) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.Blob#position(java.sql.Blob, long)
   */
  public long position(Blob pattern, long start) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.Blob#setBytes(long, byte[])
   */
  public int setBytes(long pos, byte[] bytes) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.Blob#setBytes(long, byte[], int, int)
   */
  public int setBytes(long pos, byte[] bytes, int offset, int len) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.Blob#setBinaryStream(long)
   */
  public OutputStream setBinaryStream(long pos) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.Blob#truncate(long)
   */
  public void truncate(long len) throws SQLException {
    if (len < 0)
      throw new SQLException("The length of a BLOB cannot be a negtive number.");
    if (len < this.length)
      this.length = len;
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.Blob#free()
   */
  public void free() throws SQLException {
    binaryDataChunks.clear();
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.Blob#getBinaryStream(long, long)
   */
  public InputStream getBinaryStream(long pos, long length) throws SQLException {
    return new OrientBlobInputStream(pos, length);
  }

  private class OrientBlobInputStream extends InputStream {

    private long bytesToBeRead;

    private int  positionInTheCurrentChunk;

    public OrientBlobInputStream() {
      bytesToBeRead = OrientBlob.this.length;
      positionInTheCurrentChunk = 0;
    }

    public OrientBlobInputStream(long pos, long length) {
      bytesToBeRead = length;
      positionInTheCurrentChunk = OrientBlob.this.getRelativeIndex(pos);
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.io.InputStream#read()
     */
    @Override
    public int read() throws IOException {
      if (bytesToBeRead > 0) {
        // BOUNDED READING

        // if all the bytes in the current binary chunk have been read,
        // we move to the next one
        if (positionInTheCurrentChunk == OrientBlob.this.currentChunk.length - 1) {
          // check if we've read all the available chunks
          if (OrientBlob.this.currentChunkIndex == OrientBlob.this.binaryDataChunks.size() - 1) {
            bytesToBeRead = 0;
            // we've read the last byte of the last binary chunk!
            return -1;
          } else {
            OrientBlob.this.currentChunk = OrientBlob.this.binaryDataChunks.get(++OrientBlob.this.currentChunkIndex);
            positionInTheCurrentChunk = 0;
          }
        }
        bytesToBeRead--;
        return OrientBlob.this.currentChunk[positionInTheCurrentChunk++];
      }
      return -1;
    }

  }
}
