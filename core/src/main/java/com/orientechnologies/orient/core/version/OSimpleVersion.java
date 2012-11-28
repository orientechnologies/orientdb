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
 * Implementation of standard version. This implementation contains only one integer number to hold state of version.
 *
 * @see OVersionFactory
 * @author <a href="mailto:enisher@gmail.com">Artem Orobets</a>
 */
public class OSimpleVersion implements ORecordVersion {
  public static final OBinaryConverter CONVERTER = OBinaryConverterFactory.getConverter();
  protected int                        version;

  public OSimpleVersion() {
  }

  public OSimpleVersion(int version) {
    this.version = version;
  }

  @Override
  public void increment() {
    version++;
  }

  @Override
  public void decrement() {
    version--;
  }

	@Override
	public boolean isUntracked() {
		return version == -1;
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
    return getSerializer().toString();
  }

  @Override
  public ORecordVersion copy() {
    return new OSimpleVersion(version);
  }

  @Override
  public ORecordVersionSerializer getSerializer() {
    return new OSimpleVersionSerializer();
  }

  @Override
  public int compareTo(ORecordVersion o) {
    return version - ((OSimpleVersion) o).version;
  }

  private class OSimpleVersionSerializer implements ORecordVersionSerializer {
    @Override
    public void writeTo(DataOutput out) throws IOException {
      out.writeInt(version);
    }

    @Override
    public void readFrom(DataInput in) throws IOException {
      version = in.readInt();
    }

    @Override
    public void readFrom(InputStream stream) throws IOException {
      version = OBinaryProtocol.bytes2int(stream);
    }

    @Override
    public void writeTo(OutputStream stream) throws IOException {
      stream.write(OBinaryProtocol.int2bytes(version));
    }

    @Override
    public int writeTo(byte[] iStream, int pos) {
      OBinaryProtocol.int2bytes(version, iStream, pos);
      return OBinaryProtocol.SIZE_INT;
    }

    @Override
    public int readFrom(byte[] iStream, int pos) {
      version = OBinaryProtocol.bytes2int(iStream, pos);
      return OBinaryProtocol.SIZE_INT;
    }

		@Override
		public int writeTo(OFile file, long offset) throws IOException {
			file.writeInt(offset, version);
			return OBinaryProtocol.SIZE_INT;
		}

		@Override
		public long readFrom(OFile file, long offset) throws IOException {
			version = file.readInt(offset);
			return OBinaryProtocol.SIZE_INT;
		}

		@Override
    public int fastWriteTo(byte[] iStream, int pos) {
      CONVERTER.putInt(iStream, pos, version);
      return OBinaryProtocol.SIZE_INT;
    }

    @Override
    public int fastReadFrom(byte[] iStream, int pos) {
      version = CONVERTER.getInt(iStream, pos);
      return OBinaryProtocol.SIZE_INT;
    }

    @Override
    public byte[] toByteArray() {
      final byte[] bytes = new byte[OBinaryProtocol.SIZE_INT];
      fastWriteTo(bytes, 0);
      return bytes;
    }

    @Override
    public String toString() {
      return String.valueOf(version);
    }

    @Override
    public void fromString(String string) {
      version = Integer.parseInt(string);
    }

	}
}
