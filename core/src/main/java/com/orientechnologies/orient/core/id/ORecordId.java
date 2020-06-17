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
package com.orientechnologies.orient.core.id;

import com.orientechnologies.common.util.OPatternConst;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.exception.ODatabaseException;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.serialization.OBinaryProtocol;
import com.orientechnologies.orient.core.serialization.OMemoryStream;
import com.orientechnologies.orient.core.serialization.serializer.OStringSerializerHelper;
import com.orientechnologies.orient.core.storage.OStorage;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;

public class ORecordId implements ORID {
  public static final ORecordId EMPTY_RECORD_ID = new ORecordId();
  public static final byte[] EMPTY_RECORD_ID_STREAM = EMPTY_RECORD_ID.toStream();
  public static final int PERSISTENT_SIZE = OBinaryProtocol.SIZE_SHORT + OBinaryProtocol.SIZE_LONG;
  private static final long serialVersionUID = 247070594054408657L;
  // INT TO AVOID JVM PENALTY, BUT IT'S STORED AS SHORT
  private int clusterId = CLUSTER_ID_INVALID;
  private long clusterPosition = CLUSTER_POS_INVALID;

  public ORecordId() {}

  public ORecordId(final int clusterId, final long position) {
    this.clusterId = clusterId;
    checkClusterLimits();
    clusterPosition = position;
  }

  public ORecordId(final int iClusterIdId) {
    clusterId = iClusterIdId;
    checkClusterLimits();
  }

  public ORecordId(final String iRecordId) {
    fromString(iRecordId);
  }

  /**
   * Copy constructor.
   *
   * @param parentRid Source object
   */
  public ORecordId(final ORID parentRid) {
    clusterId = parentRid.getClusterId();
    clusterPosition = parentRid.getClusterPosition();
  }

  public static String generateString(final int iClusterId, final long iPosition) {
    final StringBuilder buffer = new StringBuilder(12);
    buffer.append(PREFIX);
    buffer.append(iClusterId);
    buffer.append(SEPARATOR);
    buffer.append(iPosition);
    return buffer.toString();
  }

  public static boolean isValid(final long pos) {
    return pos != CLUSTER_POS_INVALID;
  }

  public static boolean isPersistent(final long pos) {
    return pos > CLUSTER_POS_INVALID;
  }

  public static boolean isNew(final long pos) {
    return pos < 0;
  }

  public static boolean isTemporary(final long clusterPosition) {
    return clusterPosition < CLUSTER_POS_INVALID;
  }

  public static boolean isA(final String iString) {
    return OPatternConst.PATTERN_RID.matcher(iString).matches();
  }

  public void reset() {
    clusterId = CLUSTER_ID_INVALID;
    clusterPosition = CLUSTER_POS_INVALID;
  }

  public boolean isValid() {
    return clusterPosition != CLUSTER_POS_INVALID;
  }

  public boolean isPersistent() {
    return clusterId > -1 && clusterPosition > CLUSTER_POS_INVALID;
  }

  public boolean isNew() {
    return clusterPosition < 0;
  }

  public boolean isTemporary() {
    return clusterId != -1 && clusterPosition < CLUSTER_POS_INVALID;
  }

  @Override
  public String toString() {
    return generateString(clusterId, clusterPosition);
  }

  public StringBuilder toString(StringBuilder iBuffer) {
    if (iBuffer == null) iBuffer = new StringBuilder();

    iBuffer.append(PREFIX);
    iBuffer.append(clusterId);
    iBuffer.append(SEPARATOR);
    iBuffer.append(clusterPosition);
    return iBuffer;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) return true;
    if (obj == null) return false;
    if (!(obj instanceof OIdentifiable)) return false;
    final ORecordId other = (ORecordId) ((OIdentifiable) obj).getIdentity();

    if (clusterId != other.clusterId) return false;
    return clusterPosition == other.clusterPosition;
  }

  @Override
  public int hashCode() {
    return 31 * clusterId + 103 * (int) clusterPosition;
  }

  public int compareTo(final OIdentifiable iOther) {
    if (iOther == this) return 0;

    if (iOther == null) return 1;

    final int otherClusterId = iOther.getIdentity().getClusterId();
    if (clusterId == otherClusterId) {
      final long otherClusterPos = iOther.getIdentity().getClusterPosition();

      return (clusterPosition < otherClusterPos)
          ? -1
          : ((clusterPosition == otherClusterPos) ? 0 : 1);
    } else if (clusterId > otherClusterId) return 1;

    return -1;
  }

  public int compare(final OIdentifiable iObj1, final OIdentifiable iObj2) {
    if (iObj1 == iObj2) return 0;

    if (iObj1 != null) return iObj1.compareTo(iObj2);

    return -1;
  }

  public ORecordId copy() {
    return new ORecordId(clusterId, clusterPosition);
  }

  public void toStream(final DataOutput out) throws IOException {
    out.writeShort(clusterId);
    out.writeLong(clusterPosition);
  }

  public void fromStream(final DataInput in) throws IOException {
    clusterId = in.readShort();
    clusterPosition = in.readLong();
  }

  public ORecordId fromStream(final InputStream iStream) throws IOException {
    clusterId = OBinaryProtocol.bytes2short(iStream);
    clusterPosition = OBinaryProtocol.bytes2long(iStream);
    return this;
  }

  public ORecordId fromStream(final OMemoryStream iStream) {
    clusterId = iStream.getAsShort();
    clusterPosition = iStream.getAsLong();
    return this;
  }

  public ORecordId fromStream(final byte[] iBuffer) {
    if (iBuffer != null) {
      clusterId = OBinaryProtocol.bytes2short(iBuffer, 0);
      clusterPosition = OBinaryProtocol.bytes2long(iBuffer, OBinaryProtocol.SIZE_SHORT);
    }
    return this;
  }

  public int toStream(final OutputStream iStream) throws IOException {
    final int beginOffset = OBinaryProtocol.short2bytes((short) clusterId, iStream);
    OBinaryProtocol.long2bytes(clusterPosition, iStream);
    return beginOffset;
  }

  public int toStream(final OMemoryStream iStream) throws IOException {
    final int beginOffset = OBinaryProtocol.short2bytes((short) clusterId, iStream);
    OBinaryProtocol.long2bytes(clusterPosition, iStream);
    return beginOffset;
  }

  public byte[] toStream() {
    final byte[] buffer = new byte[OBinaryProtocol.SIZE_SHORT + OBinaryProtocol.SIZE_LONG];

    OBinaryProtocol.short2bytes((short) clusterId, buffer, 0);
    OBinaryProtocol.long2bytes(clusterPosition, buffer, OBinaryProtocol.SIZE_SHORT);

    return buffer;
  }

  public int getClusterId() {
    return clusterId;
  }

  public long getClusterPosition() {
    return clusterPosition;
  }

  public void fromString(String iRecordId) {
    if (iRecordId != null) iRecordId = iRecordId.trim();

    if (iRecordId == null || iRecordId.isEmpty()) {
      clusterId = CLUSTER_ID_INVALID;
      clusterPosition = CLUSTER_POS_INVALID;
      return;
    }

    if (!OStringSerializerHelper.contains(iRecordId, SEPARATOR))
      throw new IllegalArgumentException(
          "Argument '"
              + iRecordId
              + "' is not a RecordId in form of string. Format must be: <cluster-id>:<cluster-position>");

    final List<String> parts = OStringSerializerHelper.split(iRecordId, SEPARATOR, PREFIX);

    if (parts.size() != 2)
      throw new IllegalArgumentException(
          "Argument received '"
              + iRecordId
              + "' is not a RecordId in form of string. Format must be: #<cluster-id>:<cluster-position>. Example: #3:12");

    clusterId = Integer.parseInt(parts.get(0));
    checkClusterLimits();
    clusterPosition = Long.parseLong(parts.get(1));
  }

  public void copyFrom(final ORID iSource) {
    if (iSource == null) throw new IllegalArgumentException("Source is null");

    clusterId = iSource.getClusterId();
    clusterPosition = iSource.getClusterPosition();
  }

  @Override
  public void lock(final boolean iExclusive) {
    ODatabaseRecordThreadLocal.instance()
        .get()
        .getTransaction()
        .lockRecord(
            this,
            iExclusive
                ? OStorage.LOCKING_STRATEGY.EXCLUSIVE_LOCK
                : OStorage.LOCKING_STRATEGY.SHARED_LOCK);
  }

  @Override
  public boolean isLocked() {
    return ODatabaseRecordThreadLocal.instance().get().getTransaction().isLockedRecord(this);
  }

  @Override
  public OStorage.LOCKING_STRATEGY lockingStrategy() {
    return ODatabaseRecordThreadLocal.instance().get().getTransaction().lockingStrategy(this);
  }

  @Override
  public void unlock() {
    ODatabaseRecordThreadLocal.instance().get().getTransaction().unlockRecord(this);
  }

  public String next() {
    return generateString(clusterId, clusterPosition + 1);
  }

  @Override
  public ORID nextRid() {
    return new ORecordId(clusterId, clusterPosition + 1);
  }

  public ORID getIdentity() {
    return this;
  }

  @SuppressWarnings("unchecked")
  public <T extends ORecord> T getRecord() {
    if (!isValid()) return null;

    final ODatabaseDocument db = ODatabaseRecordThreadLocal.instance().get();
    if (db == null)
      throw new ODatabaseException(
          "No database found in current thread local space. If you manually control databases over threads assure to set the current database before to use it by calling: ODatabaseRecordThreadLocal.instance().set(db);");

    return (T) db.load(this);
  }

  private void checkClusterLimits() {
    if (clusterId < -2)
      throw new ODatabaseException(
          "RecordId cannot support negative cluster id. Found: " + clusterId);

    if (clusterId > CLUSTER_MAX)
      throw new ODatabaseException(
          "RecordId cannot support cluster id major than 32767. Found: " + clusterId);
  }

  private void checkClusterLimits(int clusterId) {
    if (clusterId < -2)
      throw new ODatabaseException(
          "RecordId cannot support negative cluster id. Found: " + clusterId);

    if (clusterId > CLUSTER_MAX)
      throw new ODatabaseException(
          "RecordId cannot support cluster id major than 32767. Found: " + clusterId);
  }

  public void setClusterId(int clusterId) {
    checkClusterLimits(clusterId);

    this.clusterId = clusterId;
  }

  public void setClusterPosition(long clusterPosition) {
    this.clusterPosition = clusterPosition;
  }

  public static void serialize(ORID id, DataOutput output) throws IOException {
    if (id == null) {
      output.writeInt(-2);
      output.writeLong(-2);
    } else {
      output.writeInt(id.getClusterId());
      output.writeLong(id.getClusterPosition());
    }
  }

  public static ORecordId deserialize(DataInput input) throws IOException {
    int cluster = input.readInt();
    long pos = input.readLong();
    if (cluster == -2 && pos == -2) {
      return null;
    }
    return new ORecordId(cluster, pos);
  }
}
