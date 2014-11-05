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
package com.orientechnologies.orient.core.storage;

import com.orientechnologies.orient.core.exception.OSerializationException;
import com.orientechnologies.orient.core.serialization.OBinaryProtocol;
import com.orientechnologies.orient.core.serialization.OSerializableStream;
import com.orientechnologies.orient.core.version.ORecordVersion;
import com.orientechnologies.orient.core.version.OVersionFactory;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

public class OPhysicalPosition implements OSerializableStream, Externalizable {
  public static int      binarySize;
  private static boolean binarySizeKnown = false;
  // POSITION IN THE CLUSTER
  public long            clusterPosition;
  // TYPE
  public byte            recordType;
  // VERSION
  public ORecordVersion  recordVersion   = OVersionFactory.instance().createVersion();
  // SIZE IN BYTES OF THE RECORD. USED ONLY IN MEMORY
  public int             recordSize;

  public OPhysicalPosition() {
  }

  public OPhysicalPosition(final long iClusterPosition) {
    clusterPosition = iClusterPosition;
  }

  public OPhysicalPosition(final byte iRecordType) {
    recordType = iRecordType;
  }

  public OPhysicalPosition(final long iClusterPosition, final ORecordVersion iVersion) {
    clusterPosition = iClusterPosition;
    recordVersion.copyFrom(iVersion);
  }

  public static int binarySize() {
    if (binarySizeKnown)
      return binarySize;

    binarySizeKnown = true;
    binarySize = OBinaryProtocol.SIZE_LONG + OBinaryProtocol.SIZE_BYTE + OVersionFactory.instance().getVersionSize()
        + OBinaryProtocol.SIZE_INT;

    return binarySize;
  }

  public void copyTo(final OPhysicalPosition iDest) {
    iDest.clusterPosition = clusterPosition;
    iDest.recordType = recordType;
    iDest.recordVersion = recordVersion;
    iDest.recordSize = recordSize;
  }

  public void copyFrom(final OPhysicalPosition iSource) {
    iSource.copyTo(this);
  }

  @Override
  public String toString() {
    return "rid(?:" + clusterPosition + ") record(type:" + recordType + " size:" + recordSize + " v:" + recordVersion + ")";
  }

  public OSerializableStream fromStream(final byte[] iStream) throws OSerializationException {
    int pos = 0;

    clusterPosition = OBinaryProtocol.bytes2long(iStream);
    pos += OBinaryProtocol.SIZE_LONG;

    recordType = iStream[pos];
    pos += OBinaryProtocol.SIZE_BYTE;

    recordSize = OBinaryProtocol.bytes2int(iStream, pos);
    pos += OBinaryProtocol.SIZE_INT;

    recordVersion.getSerializer().readFrom(iStream, pos, recordVersion);

    return this;
  }

  public byte[] toStream() throws OSerializationException {
    byte[] buffer = new byte[binarySize()];
    int pos = 0;

    OBinaryProtocol.long2bytes(clusterPosition, buffer, pos);
    pos += OBinaryProtocol.SIZE_LONG;

    buffer[pos] = recordType;
    pos += OBinaryProtocol.SIZE_BYTE;

    OBinaryProtocol.int2bytes(recordSize, buffer, pos);
    pos += OBinaryProtocol.SIZE_INT;

    recordVersion.getSerializer().writeTo(buffer, pos, recordVersion);
    return buffer;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null || !(obj instanceof OPhysicalPosition))
      return false;

    final OPhysicalPosition other = (OPhysicalPosition) obj;

    return clusterPosition == other.clusterPosition && recordType == other.recordType && recordVersion.equals(other.recordVersion)
        && recordSize == other.recordSize;
  }

  @Override
  public int hashCode() {
    int result = (int) (31 * clusterPosition);
    result = 31 * result + (int) recordType;
    result = 31 * result + (recordVersion != null ? recordVersion.hashCode() : 0);
    result = 31 * result + recordSize;
    return result;
  }

  public void writeExternal(final ObjectOutput out) throws IOException {
    out.writeLong(clusterPosition);
    out.writeByte(recordType);
    out.writeInt(recordSize);
    recordVersion.getSerializer().writeTo(out, recordVersion);
  }

  public void readExternal(final ObjectInput in) throws IOException, ClassNotFoundException {
    clusterPosition = in.readLong();
    recordType = in.readByte();
    recordSize = in.readInt();
    recordVersion.getSerializer().readFrom(in, recordVersion);
  }
}
