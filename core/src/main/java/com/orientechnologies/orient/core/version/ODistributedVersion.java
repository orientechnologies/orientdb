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
import java.io.ObjectInput;
import java.io.ObjectOutput;
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
public final class ODistributedVersion implements ORecordVersion {
	public static final int STREAMED_SIZE =
					OBinaryProtocol.SIZE_INT + OBinaryProtocol.SIZE_LONG + OBinaryProtocol.SIZE_LONG;

  public static final OBinaryConverter CONVERTER = OBinaryConverterFactory.getConverter();

  private int                          counter;
  private long                         timestamp;
  private long                         macAddress;

  public ODistributedVersion() {
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
		if (isTombstone())
			throw new IllegalStateException("Record was deleted and can not be updated.");

		counter++;
    timestamp = System.currentTimeMillis();
    macAddress = OVersionFactory.instance().getMacAddress();
  }

  @Override
  public void decrement() {
		if (isTombstone())
			throw new IllegalStateException("Record was deleted and can not be updated.");

		counter--;
    timestamp = System.currentTimeMillis();
    macAddress = OVersionFactory.instance().getMacAddress();
  }

  @Override
  public boolean isUntracked() {
    return counter == -1;
  }

  @Override
  public boolean isTemporary() {
    return counter < -1;
  }

  @Override
  public boolean isValid() {
    return counter > -1;
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
  public boolean isTombstone() {
    return counter < 0;
  }

  public void convertToTombstone() {
    if (isTombstone())
      throw new IllegalStateException("Record was deleted and can not be updated.");

		counter++;
    counter = -counter;

    timestamp = System.currentTimeMillis();
    macAddress = OVersionFactory.instance().getMacAddress();
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
    return ODistributedVersionSerializer.INSTANCE;
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
    return ODistributedVersionSerializer.INSTANCE.toString(this);
  }

  @Override
  public int compareTo(ORecordVersion o) {
    ODistributedVersion other = (ODistributedVersion) o;

		final int myCounter;
		if (isTombstone())
			myCounter = -counter;
		else
		  myCounter = counter;

		final int otherCounter;
		if (o.isTombstone())
			otherCounter = -o.getCounter();
		else
			otherCounter = o.getCounter();

    if (myCounter != otherCounter)
      return myCounter > otherCounter ? 1 : -1;

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

	@Override
	public void writeExternal(ObjectOutput out) throws IOException {
		ODistributedVersionSerializer.INSTANCE.writeTo(out, this);
	}

	@Override
	public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
		ODistributedVersionSerializer.INSTANCE.readFrom(in, this);
	}

	private static final class ODistributedVersionSerializer implements ORecordVersionSerializer {
		private static final ODistributedVersionSerializer INSTANCE = new ODistributedVersionSerializer();


    @Override
    public void writeTo(DataOutput out, ORecordVersion version) throws IOException {
			final ODistributedVersion distributedVersion = (ODistributedVersion) version;

      out.writeInt(distributedVersion.counter);
      out.writeLong(distributedVersion.timestamp);
      out.writeLong(distributedVersion.macAddress);
    }

    @Override
    public void readFrom(DataInput in, ORecordVersion version) throws IOException {
			final ODistributedVersion distributedVersion = (ODistributedVersion) version;

      distributedVersion.counter = in.readInt();
			distributedVersion.timestamp = in.readLong();
			distributedVersion.macAddress = in.readLong();
    }

    @Override
    public void writeTo(OutputStream stream, ORecordVersion version) throws IOException {
			final ODistributedVersion distributedVersion = (ODistributedVersion) version;

      OBinaryProtocol.int2bytes(distributedVersion.counter, stream);
      OBinaryProtocol.long2bytes(distributedVersion.timestamp, stream);
      OBinaryProtocol.long2bytes(distributedVersion.macAddress, stream);
    }

    @Override
    public void readFrom(InputStream stream, ORecordVersion version) throws IOException {
			final ODistributedVersion distributedVersion = (ODistributedVersion) version;

			distributedVersion.counter = OBinaryProtocol.bytes2int(stream);
			distributedVersion.timestamp = OBinaryProtocol.bytes2long(stream);
			distributedVersion.macAddress = OBinaryProtocol.bytes2long(stream);
    }

    @Override
    public int writeTo(byte[] stream, int pos, ORecordVersion version) {
			final ODistributedVersion distributedVersion = (ODistributedVersion) version;

      int len = 0;
      OBinaryProtocol.int2bytes(distributedVersion.counter, stream, pos + len);
      len += OBinaryProtocol.SIZE_INT;
      OBinaryProtocol.long2bytes(distributedVersion.timestamp, stream, pos + len);
      len += OBinaryProtocol.SIZE_LONG;
      OBinaryProtocol.long2bytes(distributedVersion.macAddress, stream, pos + len);
      len += OBinaryProtocol.SIZE_LONG;

      return len;
    }

    @Override
    public int readFrom(byte[] iStream, int pos, ORecordVersion version) {
			final ODistributedVersion distributedVersion = (ODistributedVersion) version;

      int len = 0;
			distributedVersion.counter = OBinaryProtocol.bytes2int(iStream, pos + len);
      len += OBinaryProtocol.SIZE_INT;
			distributedVersion.timestamp = OBinaryProtocol.bytes2long(iStream, pos + len);
      len += OBinaryProtocol.SIZE_LONG;
			distributedVersion.macAddress = OBinaryProtocol.bytes2long(iStream, pos + len);
      len += OBinaryProtocol.SIZE_LONG;
      return len;
    }

    @Override
    public int writeTo(OFile file, long pos, ORecordVersion version) throws IOException {
			final ODistributedVersion distributedVersion = (ODistributedVersion) version;

      int len = 0;
      file.writeInt(pos + len, distributedVersion.counter);
      len += OBinaryProtocol.SIZE_INT;
      file.writeLong(pos + len, distributedVersion.timestamp);
      len += OBinaryProtocol.SIZE_LONG;
      file.writeLong(pos + len, distributedVersion.macAddress);
      len += OBinaryProtocol.SIZE_LONG;

      return len;
    }

    @Override
    public long readFrom(OFile file, long pos, ORecordVersion version) throws IOException {
			final ODistributedVersion distributedVersion = (ODistributedVersion) version;

      int len = 0;
			distributedVersion.counter = file.readInt(pos + len);
      len += OBinaryProtocol.SIZE_INT;
			distributedVersion.timestamp = file.readLong(pos + len);
      len += OBinaryProtocol.SIZE_LONG;
			distributedVersion.macAddress = file.readLong(pos + len);
      len += OBinaryProtocol.SIZE_LONG;
      return len;
    }

    @Override
    public int fastWriteTo(byte[] iStream, int pos, ORecordVersion version) {
			final ODistributedVersion distributedVersion = (ODistributedVersion) version;

      int len = 0;
      CONVERTER.putInt(iStream, pos + len, distributedVersion.counter);
      len += OBinaryProtocol.SIZE_INT;
      CONVERTER.putLong(iStream, pos + len, distributedVersion.timestamp);
      len += OBinaryProtocol.SIZE_LONG;
      CONVERTER.putLong(iStream, pos + len, distributedVersion.macAddress);
      len += OBinaryProtocol.SIZE_LONG;

      return len;
    }

    @Override
    public int fastReadFrom(byte[] iStream, int pos, ORecordVersion version) {
			final ODistributedVersion distributedVersion = (ODistributedVersion) version;

      int len = 0;
			distributedVersion.counter = CONVERTER.getInt(iStream, pos + len);
      len += OBinaryProtocol.SIZE_INT;
			distributedVersion.timestamp = CONVERTER.getLong(iStream, pos + len);
      len += OBinaryProtocol.SIZE_LONG;
			distributedVersion.macAddress = CONVERTER.getLong(iStream, pos + len);
      len += OBinaryProtocol.SIZE_LONG;
      return len;
    }

    @Override
    public byte[] toByteArray(ORecordVersion version) {
      int size = OBinaryProtocol.SIZE_INT + OBinaryProtocol.SIZE_LONG + OBinaryProtocol.SIZE_LONG;
      byte[] buffer = new byte[size];
      fastWriteTo(buffer, 0, version);
      return buffer;
    }

    @Override
    public String toString(ORecordVersion version) {
			final ODistributedVersion distributedVersion = (ODistributedVersion) version;

      return distributedVersion.counter + "." + distributedVersion.timestamp + "." + distributedVersion.macAddress;
    }

    @Override
    public void fromString(String string, ORecordVersion version) {
			final ODistributedVersion distributedVersion = (ODistributedVersion) version;

      String[] parts = string.split("\\.");
      if (parts.length != 3)
        throw new IllegalArgumentException(
            "Not correct format of distributed version. Expected <recordVersion>.<timestamp>.<macAddress>");

			distributedVersion.counter = Integer.valueOf(parts[0]);
			distributedVersion.timestamp = Long.valueOf(parts[1]);
			distributedVersion.macAddress = Long.valueOf(parts[2]);
    }
  }
}
