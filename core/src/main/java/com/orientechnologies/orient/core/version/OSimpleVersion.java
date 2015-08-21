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

package com.orientechnologies.orient.core.version;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.OutputStream;
import java.nio.ByteOrder;

import com.orientechnologies.common.serialization.OBinaryConverter;
import com.orientechnologies.common.serialization.OBinaryConverterFactory;
import com.orientechnologies.orient.core.serialization.OBinaryProtocol;
import com.orientechnologies.orient.core.storage.fs.OFile;

/**
 * Implementation of standard version. This implementation contains only one integer number to hold state of version.
 * 
 * @see OVersionFactory
 * @author Artem Orobets (enisher-at-gmail.com)
 */
public final class OSimpleVersion implements ORecordVersion {
  public static final OBinaryConverter CONVERTER = OBinaryConverterFactory.getConverter();
  protected int                        version;

  public OSimpleVersion() {
  }

  public OSimpleVersion(int version) {
    this.version = version;
  }

  @Override
  public void increment() {
    if (isTombstone())
      throw new IllegalStateException("Record was deleted and cannot be updated.");

    version++;
  }

  @Override
  public void decrement() {
    if (isTombstone())
      throw new IllegalStateException("Record was deleted and cannot be updated.");

    version--;
  }

  @Override
  public boolean isUntracked() {
    return version == -1;
  }

  @Override
  public boolean isTemporary() {
    return version < -1;
  }

  @Override
  public boolean isValid() {
    return version > -1;
  }

  @Override
  public boolean isTombstone() {
    return version < 0;
  }

  @Override
  public void convertToTombstone() {
    if (isTombstone())
      throw new IllegalStateException("Record was deleted and cannot be updated.");

    version++;
    version = -version;
  }

  @Override
  public byte[] toStream() {
    final byte[] buffer = new byte[OBinaryProtocol.SIZE_INT];
    getSerializer().writeTo(buffer, 0, this);

    return buffer;
  }

  @Override
  public void fromStream(byte[] stream) {
    getSerializer().readFrom(stream, 0, this);
  }

  @Override
  public void setCounter(int iVersion) {
    version = iVersion;
  }

  @Override
  public int getCounter() {
    return version;
  }

  @Override
  public void copyFrom(ORecordVersion iOtherVersion) {
    final OSimpleVersion otherVersion = (OSimpleVersion) iOtherVersion;

    version = otherVersion.version;
  }

  @Override
  public void reset() {
    version = 0;
  }

  @Override
  public void setRollbackMode() {
    version = Integer.MIN_VALUE + version;
  }

  @Override
  public void clearRollbackMode() {
    version = version - Integer.MIN_VALUE;
  }

  @Override
  public void disable() {
    version = -1;
  }

  @Override
  public void revive() {
    version = -version;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o)
      return true;
    if (!(o instanceof OSimpleVersion))
      return false;
    OSimpleVersion that = (OSimpleVersion) o;

    return version == that.version;
  }

  @Override
  public int hashCode() {
    return version;
  }

  @Override
  public String toString() {
    return OSimpleVersionSerializer.INSTANCE.toString(this);
  }

  @Override
  public ORecordVersion copy() {
    return new OSimpleVersion(version);
  }

  @Override
  public ORecordVersionSerializer getSerializer() {
    return OSimpleVersionSerializer.INSTANCE;
  }

  @Override
  public int compareTo(ORecordVersion o) {
    final int myVersion;
    if (isTombstone())
      myVersion = -version;
    else
      myVersion = version;

    final int otherVersion;
    if (o.isTombstone())
      otherVersion = -o.getCounter();
    else
      otherVersion = o.getCounter();

    if (myVersion == otherVersion)
      return 0;

    if (myVersion < otherVersion)
      return -1;

    return 1;
  }

  @Override
  public void writeExternal(ObjectOutput out) throws IOException {
    OSimpleVersionSerializer.INSTANCE.writeTo(out, this);
  }

  @Override
  public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
    OSimpleVersionSerializer.INSTANCE.readFrom(in, this);
  }

  private static final class OSimpleVersionSerializer implements ORecordVersionSerializer {
    private static final OSimpleVersionSerializer INSTANCE = new OSimpleVersionSerializer();

    @Override
    public void writeTo(DataOutput out, ORecordVersion version) throws IOException {
      final OSimpleVersion simpleVersion = (OSimpleVersion) version;
      out.writeInt(simpleVersion.version);
    }

    @Override
    public void readFrom(DataInput in, ORecordVersion version) throws IOException {
      final OSimpleVersion simpleVersion = (OSimpleVersion) version;
      simpleVersion.version = in.readInt();
    }

    @Override
    public void readFrom(InputStream stream, ORecordVersion version) throws IOException {
      final OSimpleVersion simpleVersion = (OSimpleVersion) version;

      simpleVersion.version = OBinaryProtocol.bytes2int(stream);
    }

    @Override
    public void writeTo(OutputStream stream, ORecordVersion version) throws IOException {
      final OSimpleVersion simpleVersion = (OSimpleVersion) version;

      stream.write(OBinaryProtocol.int2bytes(simpleVersion.version));
    }

    @Override
    public int writeTo(byte[] iStream, int pos, ORecordVersion version) {
      final OSimpleVersion simpleVersion = (OSimpleVersion) version;

      OBinaryProtocol.int2bytes(simpleVersion.version, iStream, pos);
      return OBinaryProtocol.SIZE_INT;
    }

    @Override
    public int readFrom(byte[] iStream, int pos, ORecordVersion version) {
      final OSimpleVersion simpleVersion = (OSimpleVersion) version;
      simpleVersion.version = OBinaryProtocol.bytes2int(iStream, pos);

      return OBinaryProtocol.SIZE_INT;
    }

    @Override
    public int writeTo(OFile file, long offset, ORecordVersion version) throws IOException {
      final OSimpleVersion simpleVersion = (OSimpleVersion) version;

      file.writeInt(offset, simpleVersion.version);
      return OBinaryProtocol.SIZE_INT;
    }

    @Override
    public long readFrom(OFile file, long offset, ORecordVersion version) throws IOException {
      final OSimpleVersion simpleVersion = (OSimpleVersion) version;

      simpleVersion.version = file.readInt(offset);
      return OBinaryProtocol.SIZE_INT;
    }

    @Override
    public int fastWriteTo(byte[] iStream, int pos, ORecordVersion version) {
      final OSimpleVersion simpleVersion = (OSimpleVersion) version;

      CONVERTER.putInt(iStream, pos, simpleVersion.version, ByteOrder.nativeOrder());
      return OBinaryProtocol.SIZE_INT;
    }

    @Override
    public int fastReadFrom(byte[] iStream, int pos, ORecordVersion version) {
      final OSimpleVersion simpleVersion = (OSimpleVersion) version;

      simpleVersion.version = CONVERTER.getInt(iStream, pos, ByteOrder.nativeOrder());
      return OBinaryProtocol.SIZE_INT;
    }

    @Override
    public byte[] toByteArray(ORecordVersion version) {
      final byte[] bytes = new byte[OBinaryProtocol.SIZE_INT];
      fastWriteTo(bytes, 0, version);
      return bytes;
    }

    @Override
    public String toString(ORecordVersion recordVersion) {
      final OSimpleVersion simpleVersion = (OSimpleVersion) recordVersion;

      return String.valueOf(simpleVersion.version);
    }

    @Override
    public void fromString(String string, ORecordVersion version) {
      final OSimpleVersion simpleVersion = (OSimpleVersion) version;

      simpleVersion.version = Integer.parseInt(string);
    }

  }
}
