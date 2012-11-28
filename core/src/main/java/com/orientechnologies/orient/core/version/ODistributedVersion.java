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

import com.orientechnologies.common.serialization.OBinaryConverter;
import com.orientechnologies.common.serialization.OBinaryConverterFactory;
import com.orientechnologies.orient.core.serialization.OBinaryProtocol;
import com.orientechnologies.orient.core.storage.fs.OFile;

/**
 * Implementation of {@link ORecordVersion} adapted to distributed environment. Opposite of {@link OSimpleVersion} contains
 * additional information about timestamp of last change and mac address of server that made this change.
 * 
 * @see OVersionFactory
 * @see ORecordVersion
 * 
 * @author <a href="mailto:enisher@gmail.com">Artem Orobets</a>
 */
public class ODistributedVersion implements ORecordVersion {
  public static final OBinaryConverter CONVERTER = OBinaryConverterFactory.getConverter();

  private int counter;
  private long                         timestamp;
  private long                         macAddress;

  protected ODistributedVersion() {
  }

  public ODistributedVersion(int counter) {
    this.counter = counter;
    this.timestamp = System.currentTimeMillis();
    this.macAddress = OVersionFactory.instance().getMacAddress();
  }

  public ODistributedVersion(int counter, long timestamp, long macAddress) {
    this.counter = counter;
    this.timestamp = timestamp;
    this.macAddress = macAddress;
  }

  @Override
  public void increment() {
    counter++;
    timestamp = System.currentTimeMillis();
    macAddress = OVersionFactory.instance().getMacAddress();
  }

  @Override
  public void decrement() {
    counter--;
    timestamp = System.currentTimeMillis();
    macAddress = OVersionFactory.instance().getMacAddress();
  }

  @Override
  public boolean isUntracked() {
    return counter == -1;
  }

  @Override
  public void setCounter(int iVersion) {
    counter = iVersion;
  }

  @Override
  public int getCounter() {
    return counter;
  }

  @Override
  public void copyFrom(ORecordVersion version) {
    ODistributedVersion other = (ODistributedVersion) version;
    update(other.counter, other.timestamp, other.macAddress);
  }

  public void update(int recordVersion, long timestamp, long macAddress) {
    this.counter = recordVersion;
    this.timestamp = timestamp;
    this.macAddress = macAddress;
  }

  @Override
  public void reset() {
    counter = 0;
    timestamp = System.currentTimeMillis();
    macAddress = OVersionFactory.instance().getMacAddress();
  }

  @Override
  public void setRollbackMode() {
    counter = Integer.MIN_VALUE + counter;
  }

  @Override
  public void clearRollbackMode() {
    counter = counter - Integer.MIN_VALUE;
  }

  @Override
  public void disable() {
    counter = -1;
  }

  @Override
  public void revive() {
    counter = -counter;
  }

  @Override
  public ORecordVersion copy() {
    ODistributedVersion copy = new ODistributedVersion();
    copy.counter = counter;
    copy.timestamp = timestamp;
    copy.macAddress = macAddress;
    return copy;
  }

  @Override
  public ORecordVersionSerializer getSerializer() {
    return new ODistributedVersionSerializer();
  }

  @Override
  public boolean equals(Object other) {
    return other instanceof ODistributedVersion && ((ODistributedVersion) other).compareTo(this) == 0;
  }

  @Override
  public int hashCode() {
    int result = counter;
    result = 31 * result + (int) (timestamp ^ (timestamp >>> 32));
    result = 31 * result + (int) (macAddress ^ (macAddress >>> 32));
    return result;
  }

  @Override
  public String toString() {
    return getSerializer().toString();
  }

  @Override
  public int compareTo(ORecordVersion o) {
    ODistributedVersion other = (ODistributedVersion) o;

    if (counter != other.counter)
      return counter - other.counter;
    if (timestamp != other.timestamp)
      return (timestamp > other.timestamp) ? 1 : -1;

    if (macAddress > other.macAddress)
      return 1;
    else if (macAddress < other.macAddress)
      return -1;
    else
      return 0;
  }

  public long getTimestamp() {
    return timestamp;
  }

  public long getMacAddress() {
    return macAddress;
  }

  private class ODistributedVersionSerializer implements ORecordVersionSerializer {
    @Override
    public void writeTo(DataOutput out) throws IOException {
      out.writeInt(counter);
      out.writeLong(timestamp);
      out.writeLong(macAddress);
    }

    @Override
    public void readFrom(DataInput in) throws IOException {
      counter = in.readInt();
      timestamp = in.readLong();
      macAddress = in.readLong();
    }

    @Override
    public void writeTo(OutputStream stream) throws IOException {
      OBinaryProtocol.int2bytes(counter, stream);
      OBinaryProtocol.long2bytes(timestamp, stream);
      OBinaryProtocol.long2bytes(macAddress, stream);
    }

    @Override
    public void readFrom(InputStream stream) throws IOException {
      counter = OBinaryProtocol.bytes2int(stream);
      timestamp = OBinaryProtocol.bytes2long(stream);
      macAddress = OBinaryProtocol.bytes2long(stream);
    }

    @Override
    public int writeTo(byte[] stream, int pos) {
      int len = 0;
      OBinaryProtocol.int2bytes(counter, stream, pos + len);
      len += OBinaryProtocol.SIZE_INT;
      OBinaryProtocol.long2bytes(timestamp, stream, pos + len);
      len += OBinaryProtocol.SIZE_LONG;
      OBinaryProtocol.long2bytes(macAddress, stream, pos + len);
      len += OBinaryProtocol.SIZE_LONG;

      return len;
    }

    @Override
    public int readFrom(byte[] iStream, int pos) {
      int len = 0;
      counter = OBinaryProtocol.bytes2int(iStream, pos + len);
      len += OBinaryProtocol.SIZE_INT;
      timestamp = OBinaryProtocol.bytes2long(iStream, pos + len);
      len += OBinaryProtocol.SIZE_LONG;
      macAddress = OBinaryProtocol.bytes2long(iStream, pos + len);
      len += OBinaryProtocol.SIZE_LONG;
      return len;
    }

    @Override
    public int writeTo(OFile file, long pos) throws IOException {
      int len = 0;
      file.writeInt(pos + len, counter);
      len += OBinaryProtocol.SIZE_INT;
      file.writeLong(pos + len, timestamp);
      len += OBinaryProtocol.SIZE_LONG;
      file.writeLong(pos + len, macAddress);
      len += OBinaryProtocol.SIZE_LONG;

      return len;
    }

    @Override
    public long readFrom(OFile file, long pos) throws IOException {
      int len = 0;
      counter = file.readInt(pos + len);
      len += OBinaryProtocol.SIZE_INT;
      timestamp = file.readLong(pos + len);
      len += OBinaryProtocol.SIZE_LONG;
      macAddress = file.readLong(pos + len);
      len += OBinaryProtocol.SIZE_LONG;
      return len;
    }

    @Override
    public int fastWriteTo(byte[] iStream, int pos) {
      int len = 0;
      CONVERTER.putInt(iStream, pos + len, counter);
      len += OBinaryProtocol.SIZE_INT;
      CONVERTER.putLong(iStream, pos + len, timestamp);
      len += OBinaryProtocol.SIZE_LONG;
      CONVERTER.putLong(iStream, pos + len, macAddress);
      len += OBinaryProtocol.SIZE_LONG;

      return len;
    }

    @Override
    public int fastReadFrom(byte[] iStream, int pos) {
      int len = 0;
      counter = CONVERTER.getInt(iStream, pos + len);
      len += OBinaryProtocol.SIZE_INT;
      timestamp = CONVERTER.getLong(iStream, pos + len);
      len += OBinaryProtocol.SIZE_LONG;
      macAddress = CONVERTER.getLong(iStream, pos + len);
      len += OBinaryProtocol.SIZE_LONG;
      return len;
    }

    @Override
    public byte[] toByteArray() {
      int size = OBinaryProtocol.SIZE_INT + OBinaryProtocol.SIZE_LONG + OBinaryProtocol.SIZE_LONG;
      byte[] buffer = new byte[size];
      fastWriteTo(buffer, 0);
      return buffer;
    }

    @Override
    public String toString() {
      return counter + "." + timestamp + "." + macAddress;
    }

    @Override
    public void fromString(String string) {
      String[] parts = string.split("\\.");
      if (parts.length != 3)
        throw new IllegalArgumentException(
            "Not correct format of distributed version. Expected <recordVersion>.<timestamp>.<macAddress>");

      counter = Integer.valueOf(parts[0]);
      timestamp = Long.valueOf(parts[1]);
      macAddress = Long.valueOf(parts[2]);
    }
  }
}
