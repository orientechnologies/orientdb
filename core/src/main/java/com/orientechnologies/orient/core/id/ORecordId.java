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
package com.orientechnologies.orient.core.id;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;

import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.exception.ODatabaseException;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.serialization.OBinaryProtocol;
import com.orientechnologies.orient.core.serialization.OMemoryStream;
import com.orientechnologies.orient.core.serialization.serializer.OStringSerializerHelper;

public class ORecordId implements ORID {
  private static final long     serialVersionUID       = 247070594054408657L;

  public static final ORecordId EMPTY_RECORD_ID        = new ORecordId();
  public static final byte[]    EMPTY_RECORD_ID_STREAM = EMPTY_RECORD_ID.toStream();

  public int                    clusterId              = CLUSTER_ID_INVALID;                                      // INT TO AVOID
                                                                                                                   // JVM
                                                                                                                   // PENALITY, BUT
                                                                                                                   // IT'S STORED
                                                                                                                   // AS SHORT
  public OClusterPosition       clusterPosition        = OClusterPosition.INVALID_POSITION;
  public static final int       PERSISTENT_SIZE        = OBinaryProtocol.SIZE_SHORT
                                                           + OClusterPositionFactory.INSTANCE.getSerializedSize();

  public ORecordId() {
  }

  public ORecordId(final int iClusterId, final OClusterPosition iPosition) {
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
   * @param parentRid
   *          Source object
   */
  public ORecordId(final ORID parentRid) {
    clusterId = parentRid.getClusterId();
    clusterPosition = parentRid.getClusterPosition();
  }

  public void reset() {
    clusterId = CLUSTER_ID_INVALID;
    clusterPosition = CLUSTER_POS_INVALID;
  }

  public boolean isValid() {
    return clusterPosition.isValid();
  }

  public boolean isPersistent() {
    return clusterId > -1 && clusterPosition.isPersistent();
  }

  public boolean isNew() {
    return clusterPosition.isNew();
  }

  public boolean isTemporary() {
    return clusterId != -1 && clusterPosition.isTemporary();
  }

  @Override
  public String toString() {
    return generateString(clusterId, clusterPosition);
  }

  public StringBuilder toString(StringBuilder iBuffer) {
    if (iBuffer == null)
      iBuffer = new StringBuilder();

    iBuffer.append(PREFIX);
    iBuffer.append(clusterId);
    iBuffer.append(SEPARATOR);
    iBuffer.append(clusterPosition);
    return iBuffer;
  }

  public static String generateString(final int iClusterId, final OClusterPosition iPosition) {
    final StringBuilder buffer = new StringBuilder(12);
    buffer.append(PREFIX);
    buffer.append(iClusterId);
    buffer.append(SEPARATOR);
    buffer.append(iPosition);
    return buffer.toString();
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

    if (clusterId != other.clusterId)
      return false;
    if (!clusterPosition.equals(other.clusterPosition))
      return false;
    return true;
  }

  @Override
  public int hashCode() {
    int result = clusterId;
    result = 31 * result + clusterPosition.hashCode();
    return result;
  }

  public int compareTo(final OIdentifiable iOther) {
    if (iOther == this)
      return 0;

    if (iOther == null)
      return 1;

    final int otherClusterId = iOther.getIdentity().getClusterId();
    if (clusterId == otherClusterId) {
      final OClusterPosition otherClusterPos = iOther.getIdentity().getClusterPosition();

      return clusterPosition.compareTo(otherClusterPos);
    } else if (clusterId > otherClusterId)
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
    return new ORecordId(clusterId, clusterPosition);
  }

  private void checkClusterLimits() {
    if (clusterId < -2)
      throw new ODatabaseException("RecordId cannot support negative cluster id. You've used: " + clusterId);

    if (clusterId > CLUSTER_MAX)
      throw new ODatabaseException("RecordId cannot support cluster id major than 32767. You've used: " + clusterId);
  }

  public ORecordId fromStream(final InputStream iStream) throws IOException {
    clusterId = OBinaryProtocol.bytes2short(iStream);

    clusterPosition = OClusterPositionFactory.INSTANCE.fromStream(iStream);
    return this;
  }

  public ORecordId fromStream(final OMemoryStream iStream) {
    clusterId = iStream.getAsShort();
    clusterPosition = OClusterPositionFactory.INSTANCE.fromStream(iStream.getAsByteArrayFixed(OClusterPositionFactory.INSTANCE
        .getSerializedSize()));
    return this;
  }

  public ORecordId fromStream(final byte[] iBuffer) {
    if (iBuffer != null) {
      clusterId = OBinaryProtocol.bytes2short(iBuffer, 0);

      clusterPosition = OClusterPositionFactory.INSTANCE.fromStream(iBuffer, OBinaryProtocol.SIZE_SHORT);
    }
    return this;
  }

  public int toStream(final OutputStream iStream) throws IOException {
    final int beginOffset = OBinaryProtocol.short2bytes((short) clusterId, iStream);
    iStream.write(clusterPosition.toStream());
    return beginOffset;
  }

  public int toStream(final OMemoryStream iStream) throws IOException {
    final int beginOffset = OBinaryProtocol.short2bytes((short) clusterId, iStream);
    iStream.write(clusterPosition.toStream());
    return beginOffset;
  }

  public byte[] toStream() {
    final int serializedSize = OClusterPositionFactory.INSTANCE.getSerializedSize();

    byte[] buffer = new byte[OBinaryProtocol.SIZE_SHORT + serializedSize];

    OBinaryProtocol.short2bytes((short) clusterId, buffer, 0);
    System.arraycopy(clusterPosition.toStream(), 0, buffer, OBinaryProtocol.SIZE_SHORT, serializedSize);

    return buffer;
  }

  public int getClusterId() {
    return clusterId;
  }

  public OClusterPosition getClusterPosition() {
    return clusterPosition;
  }

  public void fromString(String iRecordId) {
    if (iRecordId != null)
      iRecordId = iRecordId.trim();

    if (iRecordId == null || iRecordId.isEmpty()) {
      clusterId = CLUSTER_ID_INVALID;
      clusterPosition = CLUSTER_POS_INVALID;
      return;
    }

    if (!OStringSerializerHelper.contains(iRecordId, SEPARATOR))
      throw new IllegalArgumentException("Argument '" + iRecordId
          + "' is not a RecordId in form of string. Format must be: <cluster-id>:<cluster-position>");

    final List<String> parts = OStringSerializerHelper.split(iRecordId, SEPARATOR, PREFIX);

    if (parts.size() != 2)
      throw new IllegalArgumentException("Argument received '" + iRecordId
          + "' is not a RecordId in form of string. Format must be: #<cluster-id>:<cluster-position>. Example: #3:12");

    clusterId = Integer.parseInt(parts.get(0));
    checkClusterLimits();
    clusterPosition = OClusterPositionFactory.INSTANCE.valueOf(parts.get(1));
  }

  public void copyFrom(final ORID iSource) {
    if (iSource == null)
      throw new IllegalArgumentException("Source is null");

    clusterId = iSource.getClusterId();
    clusterPosition = iSource.getClusterPosition();
  }

  public String next() {
    return generateString(clusterId, clusterPosition.inc());
  }

  @Override
  public ORID nextRid() {
    return new ORecordId(clusterId, clusterPosition.inc());
  }

  public ORID getIdentity() {
    return this;
  }

  @SuppressWarnings("unchecked")
  public <T extends ORecord<?>> T getRecord() {
    if (!isValid())
      return null;

    final ODatabaseRecord db = ODatabaseRecordThreadLocal.INSTANCE.get();
    if (db == null)
      throw new ODatabaseException(
          "No database found in current thread local space. If you manually control databases over threads assure to set the current database before to use it by calling: ODatabaseRecordThreadLocal.INSTANCE.set(db);");

    return (T) db.load(this);
  }
}
