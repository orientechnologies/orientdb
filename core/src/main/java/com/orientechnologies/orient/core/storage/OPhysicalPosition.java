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
package com.orientechnologies.orient.core.storage;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import com.orientechnologies.orient.core.exception.OSerializationException;
import com.orientechnologies.orient.core.id.OClusterPosition;
import com.orientechnologies.orient.core.id.OClusterPositionFactory;
import com.orientechnologies.orient.core.serialization.OBinaryProtocol;
import com.orientechnologies.orient.core.serialization.OSerializableStream;
import com.orientechnologies.orient.core.version.ORecordVersion;
import com.orientechnologies.orient.core.version.OVersionFactory;

public class OPhysicalPosition implements OSerializableStream, Comparable<OPhysicalPosition>, Externalizable {
  // POSITION IN THE CLUSTER
  public OClusterPosition clusterPosition;
  // ID OF DATA SEGMENT
  public int              dataSegmentId;
  // POSITION OF CHUNK EXPRESSES AS OFFSET IN BYTES INSIDE THE DATA SEGMENT
  public long             dataSegmentPos;
  // TYPE
  public byte             recordType;
  // VERSION
  public ORecordVersion   recordVersion   = OVersionFactory.instance().createVersion();
  // SIZE IN BYTES OF THE RECORD. USED ONLY IN MEMORY
  public int              recordSize;

  public static int       binarySize;
  private static boolean  binarySizeKnown = false;

  public OPhysicalPosition() {
  }

  public OPhysicalPosition(final OClusterPosition iClusterPosition) {
    clusterPosition = iClusterPosition;
  }

  public OPhysicalPosition(final int iDataSegmentId, final long iDataSegmentPosition, final byte iRecordType) {
    dataSegmentId = iDataSegmentId;
    dataSegmentPos = iDataSegmentPosition;
    recordType = iRecordType;
  }

  public OPhysicalPosition(final OClusterPosition iClusterPosition, final ORecordVersion iVersion) {
    clusterPosition = iClusterPosition;
    recordVersion.copyFrom(iVersion);
  }

  public void copyTo(final OPhysicalPosition iDest) {
    iDest.clusterPosition = clusterPosition;
    iDest.dataSegmentId = dataSegmentId;
    iDest.dataSegmentPos = dataSegmentPos;
    iDest.recordType = recordType;
    iDest.recordVersion = recordVersion;
    iDest.recordSize = recordSize;
  }

  public void copyFrom(final OPhysicalPosition iSource) {
    iSource.copyTo(this);
  }

  @Override
  public String toString() {
    return "rid(?:" + clusterPosition + ") data(" + dataSegmentId + ":" + dataSegmentPos + ") record(type:" + recordType + " size:"
        + recordSize + " v:" + recordVersion + ")";
  }

  public OSerializableStream fromStream(final byte[] iStream) throws OSerializationException {
    int pos = 0;

    clusterPosition = OClusterPositionFactory.INSTANCE.fromStream(iStream);
    pos += OClusterPositionFactory.INSTANCE.getSerializedSize();

    dataSegmentId = OBinaryProtocol.bytes2int(iStream, pos);
    pos += OBinaryProtocol.SIZE_INT;

    dataSegmentPos = OBinaryProtocol.bytes2long(iStream, pos);
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

    final byte[] clusterContent = clusterPosition.toStream();
    System.arraycopy(clusterContent, 0, buffer, 0, clusterContent.length);
    pos += clusterContent.length;

    OBinaryProtocol.int2bytes(dataSegmentId, buffer, pos);
    pos += OBinaryProtocol.SIZE_INT;

    OBinaryProtocol.long2bytes(dataSegmentPos, buffer, pos);
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

    return clusterPosition.equals(other.clusterPosition) && recordType == other.recordType
        && recordVersion.equals(other.recordVersion) && recordSize == other.recordSize;
  }

  @Override
  public int hashCode() {
    int result = clusterPosition != null ? clusterPosition.hashCode() : 0;
    result = 31 * result + dataSegmentId;
    result = 31 * result + (int) (dataSegmentPos ^ (dataSegmentPos >>> 32));
    result = 31 * result + (int) recordType;
    result = 31 * result + (recordVersion != null ? recordVersion.hashCode() : 0);
    result = 31 * result + recordSize;
    return result;
  }

  public int compareTo(final OPhysicalPosition iOther) {
    return (int) (dataSegmentPos - iOther.dataSegmentPos);
  }

  public void writeExternal(final ObjectOutput out) throws IOException {
    final byte[] clusterContent = clusterPosition.toStream();
    out.write(clusterContent);

    out.writeInt(dataSegmentId);
    out.writeLong(dataSegmentPos);
    out.writeByte(recordType);
    out.writeInt(recordSize);
    recordVersion.getSerializer().writeTo(out, recordVersion);
  }

  public void readExternal(final ObjectInput in) throws IOException, ClassNotFoundException {
    clusterPosition = OClusterPositionFactory.INSTANCE.fromStream(in);

    dataSegmentId = in.readInt();
    dataSegmentPos = in.readLong();
    recordType = in.readByte();
    recordSize = in.readInt();
    recordVersion.getSerializer().readFrom(in, recordVersion);
  }

  public static int binarySize() {
    if (binarySizeKnown)
      return binarySize;

    binarySizeKnown = true;
    binarySize = OClusterPositionFactory.INSTANCE.getSerializedSize() + OBinaryProtocol.SIZE_INT + OBinaryProtocol.SIZE_LONG
        + OBinaryProtocol.SIZE_BYTE + OVersionFactory.instance().getVersionSize() + OBinaryProtocol.SIZE_INT;

    return binarySize;
  }
}
