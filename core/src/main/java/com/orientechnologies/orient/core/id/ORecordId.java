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
package com.orientechnologies.orient.core.id;

import java.io.*;
import java.util.List;

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

public class ORecordId implements ORID {
  public static final  ORecordId EMPTY_RECORD_ID        = new ORecordId();
  public static final  byte[]    EMPTY_RECORD_ID_STREAM = EMPTY_RECORD_ID.toStream();
  public static final  int       PERSISTENT_SIZE        = OBinaryProtocol.SIZE_SHORT + OBinaryProtocol.SIZE_LONG;
  private static final long      serialVersionUID       = 247070594054408657L;
  // INT TO AVOID JVM PENALTY, BUT IT'S STORED AS SHORT
  private              int       clusterId              = CLUSTER_ID_INVALID;
  private              long      clusterPosition        = CLUSTER_POS_INVALID;

  public ORecordId() {
  }

  public ORecordId(final int iClusterId, final long iPosition) {
    clusterId = iClusterId;
    checkClusterLimits();
    clusterPosition = iPosition;
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
    this.clusterId = parentRid.getClusterId();
    this.clusterPosition = parentRid.getClusterPosition();
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
    return getClusterPosition() != CLUSTER_POS_INVALID;
  }

  public boolean isPersistent() {
    return getClusterId() > -1 && getClusterPosition() > CLUSTER_POS_INVALID;
  }

  public boolean isNew() {
    return getClusterPosition() < 0;
  }

  public boolean isTemporary() {
    return getClusterId() != -1 && getClusterPosition() < CLUSTER_POS_INVALID;
  }

  @Override
  public String toString() {
    return generateString(getClusterId(), getClusterPosition());
  }

  public StringBuilder toString(StringBuilder iBuffer) {
    if (iBuffer == null)
      iBuffer = new StringBuilder();

    iBuffer.append(PREFIX);
    iBuffer.append(clusterId);
    iBuffer.append(SEPARATOR);
    iBuffer.append(getClusterPosition());
    return iBuffer;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (!(obj instanceof OIdentifiable))
      return false;
    final ORecordId other = (ORecordId) ((OIdentifiable) obj).getIdentity();

    if (getClusterId() != other.getClusterId())
      return false;
    if (getClusterPosition() != other.getClusterPosition())
      return false;
    return true;
  }

  @Override
  public int hashCode() {
    return 31 * getClusterId() + 103 * (int) getClusterPosition();
  }

  public int compareTo(final OIdentifiable iOther) {
    if (iOther == this)
      return 0;

    if (iOther == null)
      return 1;

    final int otherClusterId = iOther.getIdentity().getClusterId();
    if (getClusterId() == otherClusterId) {
      final long otherClusterPos = iOther.getIdentity().getClusterPosition();

      return (getClusterPosition() < otherClusterPos) ? -1 : ((getClusterPosition() == otherClusterPos) ? 0 : 1);
    } else if (getClusterId() > otherClusterId)
      return 1;

    return -1;
  }

  public int compare(final OIdentifiable iObj1, final OIdentifiable iObj2) {
    if (iObj1 == iObj2)
      return 0;

    if (iObj1 != null)
      return iObj1.compareTo(iObj2);

    return -1;
  }

  public ORecordId copy() {
    return new ORecordId(getClusterId(), getClusterPosition());
  }

  public void toStream(final DataOutput out) throws IOException {
    out.writeShort(getClusterId());
    out.writeLong(getClusterPosition());
  }

  public void fromStream(final DataInput in) throws IOException {
    setClusterId(in.readShort());
    setClusterPosition(in.readLong());
  }

  public ORecordId fromStream(final InputStream iStream) throws IOException {
    setClusterId(OBinaryProtocol.bytes2short(iStream));
    setClusterPosition(OBinaryProtocol.bytes2long(iStream));
    return this;
  }

  public ORecordId fromStream(final OMemoryStream iStream) {
    setClusterId(iStream.getAsShort());
    setClusterPosition(iStream.getAsLong());
    return this;
  }

  public ORecordId fromStream(final byte[] iBuffer) {
    if (iBuffer != null) {
      setClusterId(OBinaryProtocol.bytes2short(iBuffer, 0));
      setClusterPosition(OBinaryProtocol.bytes2long(iBuffer, OBinaryProtocol.SIZE_SHORT));
    }
    return this;
  }

  public int toStream(final OutputStream iStream) throws IOException {
    final int beginOffset = OBinaryProtocol.short2bytes((short) getClusterId(), iStream);
    OBinaryProtocol.long2bytes(getClusterPosition(), iStream);
    return beginOffset;
  }

  public int toStream(final OMemoryStream iStream) throws IOException {
    final int beginOffset = OBinaryProtocol.short2bytes((short) getClusterId(), iStream);
    OBinaryProtocol.long2bytes(getClusterPosition(), iStream);
    return beginOffset;
  }

  public byte[] toStream() {
    final byte[] buffer = new byte[OBinaryProtocol.SIZE_SHORT + OBinaryProtocol.SIZE_LONG];

    OBinaryProtocol.short2bytes((short) getClusterId(), buffer, 0);
    OBinaryProtocol.long2bytes(getClusterPosition(), buffer, OBinaryProtocol.SIZE_SHORT);

    return buffer;
  }

  public int getClusterId() {
    return clusterId;
  }

  public long getClusterPosition() {
    return clusterPosition;
  }

  public void fromString(String iRecordId) {
    if (iRecordId != null)
      iRecordId = iRecordId.trim();

    if (iRecordId == null || iRecordId.isEmpty()) {
      setClusterId(CLUSTER_ID_INVALID);
      setClusterPosition(CLUSTER_POS_INVALID);
      return;
    }

    if (!OStringSerializerHelper.contains(iRecordId, SEPARATOR))
      throw new IllegalArgumentException(
          "Argument '" + iRecordId + "' is not a RecordId in form of string. Format must be: <cluster-id>:<cluster-position>");

    final List<String> parts = OStringSerializerHelper.split(iRecordId, SEPARATOR, PREFIX);

    if (parts.size() != 2)
      throw new IllegalArgumentException("Argument received '" + iRecordId
          + "' is not a RecordId in form of string. Format must be: #<cluster-id>:<cluster-position>. Example: #3:12");

    setClusterId(Integer.parseInt(parts.get(0)));
    checkClusterLimits();
    setClusterPosition(Long.parseLong(parts.get(1)));
  }

  public void copyFrom(final ORID iSource) {
    if (iSource == null)
      throw new IllegalArgumentException("Source is null");

    setClusterId(iSource.getClusterId());
    setClusterPosition(iSource.getClusterPosition());
  }

  @Override
  public void lock(final boolean iExclusive) {
    ODatabaseRecordThreadLocal.INSTANCE.get().getTransaction()
        .lockRecord(this, iExclusive ? OStorage.LOCKING_STRATEGY.EXCLUSIVE_LOCK : OStorage.LOCKING_STRATEGY.SHARED_LOCK);
  }

  @Override
  public boolean isLocked() {
    return ODatabaseRecordThreadLocal.INSTANCE.get().getTransaction().isLockedRecord(this);
  }

  @Override
  public OStorage.LOCKING_STRATEGY lockingStrategy() {
    return ODatabaseRecordThreadLocal.INSTANCE.get().getTransaction().lockingStrategy(this);
  }

  @Override
  public void unlock() {
    ODatabaseRecordThreadLocal.INSTANCE.get().getTransaction().unlockRecord(this);
  }

  public String next() {
    return generateString(getClusterId(), getClusterPosition() + 1);
  }

  @Override
  public ORID nextRid() {
    return new ORecordId(getClusterId(), getClusterPosition() + 1);
  }

  public ORID getIdentity() {
    return this;
  }

  @SuppressWarnings("unchecked")
  public <T extends ORecord> T getRecord() {
    if (!isValid())
      return null;

    final ODatabaseDocument db = ODatabaseRecordThreadLocal.INSTANCE.get();
    if (db == null)
      throw new ODatabaseException(
          "No database found in current thread local space. If you manually control databases over threads assure to set the current database before to use it by calling: ODatabaseRecordThreadLocal.INSTANCE.set(db);");

    return (T) db.load(this);
  }

  private void checkClusterLimits() {
    if (getClusterId() < -2)
      throw new ODatabaseException("RecordId cannot support negative cluster id. Found: " + getClusterId());

    if (getClusterId() > CLUSTER_MAX)
      throw new ODatabaseException("RecordId cannot support cluster id major than 32767. Found: " + getClusterId());
  }

  private void checkClusterLimits(int clusterId) {
    if (clusterId < -2)
      throw new ODatabaseException("RecordId cannot support negative cluster id. Found: " + getClusterId());

    if (clusterId > CLUSTER_MAX)
      throw new ODatabaseException("RecordId cannot support cluster id major than 32767. Found: " + getClusterId());
  }

  public void setClusterId(int clusterId) {
    checkClusterLimits(clusterId);

    this.clusterId = clusterId;
  }

  public void setClusterPosition(long clusterPosition) {
    this.clusterPosition = clusterPosition;
  }
}
