/*
 *
 *  *  Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
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
 *  * For more information: http://orientdb.com
 *
 */
package com.orientechnologies.orient.core.storage;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

public class OPhysicalPosition implements Externalizable {
  public long clusterPosition;
  public byte recordType;
  public int recordVersion = 0;
  public int recordSize;

  public OPhysicalPosition() {}

  public OPhysicalPosition(final long iClusterPosition) {
    clusterPosition = iClusterPosition;
  }

  public OPhysicalPosition(final byte iRecordType) {
    recordType = iRecordType;
  }

  public OPhysicalPosition(final long iClusterPosition, final int iVersion) {
    clusterPosition = iClusterPosition;
    recordVersion = iVersion;
  }

  private void copyTo(final OPhysicalPosition iDest) {
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
    return "rid(?:"
        + clusterPosition
        + ") record(type:"
        + recordType
        + " size:"
        + recordSize
        + " v:"
        + recordVersion
        + ")";
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null || !(obj instanceof OPhysicalPosition)) return false;

    final OPhysicalPosition other = (OPhysicalPosition) obj;

    return clusterPosition == other.clusterPosition
        && recordType == other.recordType
        && recordVersion == other.recordVersion
        && recordSize == other.recordSize;
  }

  @Override
  public int hashCode() {
    int result = (int) (31 * clusterPosition);
    result = 31 * result + (int) recordType;
    result = 31 * result + recordVersion;
    result = 31 * result + recordSize;
    return result;
  }

  @Override
  public void writeExternal(final ObjectOutput out) throws IOException {
    out.writeLong(clusterPosition);
    out.writeByte(recordType);
    out.writeInt(recordSize);
    out.writeInt(recordVersion);
  }

  @Override
  public void readExternal(final ObjectInput in) throws IOException, ClassNotFoundException {
    clusterPosition = in.readLong();
    recordType = in.readByte();
    recordSize = in.readInt();
    recordVersion = in.readInt();
  }
}
