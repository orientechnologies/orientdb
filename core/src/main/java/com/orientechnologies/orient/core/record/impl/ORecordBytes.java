/*
 *
 *  *  Copyright 2014 Orient Technologies LTD (info(at)orientechnologies.com)
 *  *
 *  *  Licensed under the Apache License, Version 2.0 (the "License");
 *  *  you may not use this file except in compliance with the License.
 *  *  You may obtain a copy of the License at
 *  *
 *  *       http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  *  Unless required by applicable law or agreed to in writing, software
 *  *  distributed under the License is distributed on an "AS IS" BASIS,
 *  *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  *  See the License for the specific language governing permissions and
 *  *  limitations under the License.
 *  *
 *  * For more information: http://www.orientechnologies.com
 *
 */
package com.orientechnologies.orient.core.record.impl;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;

import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.record.ORecordElement;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.record.ORecordAbstract;
import com.orientechnologies.orient.core.serialization.OMemoryStream;
import com.orientechnologies.orient.core.serialization.serializer.record.ORecordSerializerFactory;
import com.orientechnologies.orient.core.serialization.serializer.record.ORecordSerializerRaw;

/**
 * The rawest representation of a record. It's schema less. Use this if you need to store Strings or byte[] without matter about the
 * content. Useful also to store multimedia contents and binary files. The object can be reused across calls to the database by
 * using the reset() at every re-use.
 */
@SuppressWarnings({ "unchecked" })
public class ORecordBytes extends ORecordAbstract {
  private static final long   serialVersionUID = 1L;

  public static final byte    RECORD_TYPE      = 'b';
  private static final byte[] EMPTY_SOURCE     = new byte[] {};

  public ORecordBytes() {
    setup();
  }

  public ORecordBytes(final ODatabaseDocumentInternal iDatabase) {
    setup();
    ODatabaseRecordThreadLocal.INSTANCE.set(iDatabase);
  }

  public ORecordBytes(final ODatabaseDocumentInternal iDatabase, final byte[] iSource) {
    this(iSource);
    ODatabaseRecordThreadLocal.INSTANCE.set(iDatabase);
  }

  public ORecordBytes(final byte[] iSource) {
    super(iSource);
    _dirty = true;
    _contentChanged = true;
    setup();
  }

  public ORecordBytes(final ORID iRecordId) {
    _recordId = (ORecordId) iRecordId;
    setup();
  }

  public ORecordBytes reset(final byte[] iSource) {
    reset();
    _source = iSource;
    return this;
  }

  public ORecordBytes copy() {
    return (ORecordBytes) copyTo(new ORecordBytes());
  }

  @Override
  public ORecordBytes fromStream(final byte[] iRecordBuffer) {
    _source = iRecordBuffer;
    _status = ORecordElement.STATUS.LOADED;
    return this;
  }

  @Override
  public byte[] toStream() {
    return _source;
  }

  public byte getRecordType() {
    return RECORD_TYPE;
  }

  @Override
  protected void setup() {
    super.setup();
    _recordFormat = ORecordSerializerFactory.instance().getFormat(ORecordSerializerRaw.NAME);
  }

  /**
   * Reads the input stream in memory. This is less efficient than {@link #fromInputStream(InputStream, int)} because allocation is
   * made multiple times. If you already know the input size use {@link #fromInputStream(InputStream, int)}.
   * 
   * @param in
   *          Input Stream, use buffered input stream wrapper to speed up reading
   * @return Buffer read from the stream. It's also the internal buffer size in bytes
   * @throws IOException
   */
  public int fromInputStream(final InputStream in) throws IOException {
    final OMemoryStream out = new OMemoryStream();
    try {
      final byte[] buffer = new byte[OMemoryStream.DEF_SIZE];
      int readBytesCount;
      while (true) {
        readBytesCount = in.read(buffer, 0, buffer.length);
        if (readBytesCount == -1) {
          break;
        }
        out.write(buffer, 0, readBytesCount);
      }
      out.flush();
      _source = out.toByteArray();
    } finally {
      out.close();
    }
    _size = _source.length;
    return _size;
  }

  /**
   * Reads the input stream in memory specifying the maximum bytes to read. This is more efficient than
   * {@link #fromInputStream(InputStream)} because allocation is made only once.
   * 
   * @param in
   *          Input Stream, use buffered input stream wrapper to speed up reading
   * @param maxSize
   *          Maximum size to read
   * @return Buffer count of bytes that are read from the stream. It's also the internal buffer size in bytes
   * @throws IOException
   *           if an I/O error occurs.
   */
  public int fromInputStream(final InputStream in, final int maxSize) throws IOException {
    final byte[] buffer = new byte[maxSize];
    int totalBytesCount = 0;
    int readBytesCount;
    while (totalBytesCount < maxSize) {
      readBytesCount = in.read(buffer, totalBytesCount, buffer.length - totalBytesCount);
      if (readBytesCount == -1) {
        break;
      }
      totalBytesCount += readBytesCount;
    }

    if (totalBytesCount == 0) {
      _source = EMPTY_SOURCE;
      _size = 0;
    } else if (totalBytesCount == maxSize) {
      _source = buffer;
      _size = maxSize;
    } else {
      _source = Arrays.copyOf(buffer, totalBytesCount);
      _size = totalBytesCount;
    }
    return _size;
  }

  public void toOutputStream(final OutputStream out) throws IOException {
    checkForLoading();

    if (_source.length > 0) {
      out.write(_source);
    }
  }
}
