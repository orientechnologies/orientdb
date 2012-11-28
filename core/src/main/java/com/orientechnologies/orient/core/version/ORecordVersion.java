/*
 * Copyright 2010-2012 Luca Garulli (l.garulli--at--orientechnologies.com)
 *
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

package com.orientechnologies.orient.core.version;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import com.orientechnologies.orient.core.storage.fs.OFile;

/**
 * Abstraction for record version. In non distributed environment it is just a number represented by {@link OSimpleVersion}. In
 * distributed environment records can be processed concurrently, so simple number is not enough, as we need some additional
 * information to prevent generation of two different versions of document with the same version number. This interface provide an
 * ability to create extended versions.
 * 
 * @author <a href="mailto:enisher@gmail.com">Artem Orobets</a>
 * @see OSimpleVersion
 * @see ODistributedVersion
 */
public interface ORecordVersion extends Comparable<ORecordVersion> {
  void increment();

  void decrement();

  boolean isUntracked();

  void setCounter(int iVersion);

  int getCounter();

  void copyFrom(ORecordVersion version);

  void reset();

  void setRollbackMode();

  void clearRollbackMode();

  void disable();

  void revive();

  ORecordVersion copy();

  ORecordVersionSerializer getSerializer();

  boolean equals(Object other);

  int hashCode();

  String toString();

  /**
   * Provides serialization to different sources.
   */
  public interface ORecordVersionSerializer {
    void writeTo(DataOutput out) throws IOException;

    void readFrom(DataInput in) throws IOException;

    void readFrom(InputStream stream) throws IOException;

    void writeTo(OutputStream stream) throws IOException;

    /**
     * Writes version to stream.
     * 
     * @param iStream
     *          stream to write data.
     * @param pos
     *          the beginning index, inclusive.
     * @return size of serialized object
     */
    int writeTo(byte[] iStream, int pos);

    /**
     * Reads version from stream.
     * 
     * @param iStream
     *          stream that contains serialized data.
     * @param pos
     *          the beginning index, inclusive.
     * @return size of deserialized object
     */
    int readFrom(byte[] iStream, int pos);

    int writeTo(OFile file, long offset) throws IOException;

    long readFrom(OFile file, long offset) throws IOException;

    /**
     * The same as {@link #writeTo(byte[], int)}, but uses platform dependent optimization to speed up writing.
     * 
     * @param iStream
     * @param pos
     * @return size of serialized object
     */
    int fastWriteTo(byte[] iStream, int pos);

    /**
     * The same as {@link #readFrom(byte[], int)}, but uses platform dependent optimization to speed up reading.
     * 
     * @param iStream
     * @param pos
     * @return size of deserialized object
     */
    int fastReadFrom(byte[] iStream, int pos);

    /**
     * Can use platform dependant optimization.
     * 
     * @return serialized version
     */
    byte[] toByteArray();

    String toString();

    void fromString(String string);
  }
}
