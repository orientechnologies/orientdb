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

package com.orientechnologies.orient.core.storage.impl.local.paginated.wal;

import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.cas.OWriteableWALRecord;

import java.util.concurrent.atomic.AtomicReference;

/**
 * Abstract WAL record.
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 * @since 12.12.13
 */
public abstract class OAbstractWALRecord implements OWriteableWALRecord {
  private int distance = -1;
  private int diskSize = -1;

  private byte[] binaryContent;

  protected final AtomicReference<OLogSequenceNumber> lsn = new AtomicReference<>();

  protected OAbstractWALRecord() {
  }

  protected OAbstractWALRecord(final OLogSequenceNumber previousCheckpoint) {
    this.lsn.set(previousCheckpoint);
  }

  @Override
  public OLogSequenceNumber getLsn() {
    return lsn.get();
  }

  @Override
  public void setLsn(final OLogSequenceNumber lsn) {
    this.lsn.set(lsn);
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;

    final OAbstractWALRecord that = (OAbstractWALRecord) o;

    if (lsn != null ? !lsn.equals(that.lsn) : that.lsn != null)
      return false;

    return true;
  }

  @Override
  public void setBinaryContent(byte[] content) {
    this.binaryContent = binaryContent;
  }

  @Override
  public byte[] getBinaryContent() {
    return binaryContent;
  }

  @Override
  public boolean casLSN(OLogSequenceNumber currentLSN, OLogSequenceNumber newLSN) {
    return lsn.compareAndSet(currentLSN, newLSN);
  }

  @Override
  public void setDistance(int distance) {
    this.distance = distance;
  }

  @Override
  public void setDiskSize(int diskSize) {
    this.diskSize = diskSize;
  }

  @Override
  public int getDistance() {
    if (distance < 0) {
      throw new IllegalStateException("Distance is not set");
    }

    return distance;
  }

  @Override
  public int getDiskSize() {
    if (diskSize < 0) {
      throw new IllegalStateException("Disk size is not set");
    }

    return diskSize;
  }

  @Override
  public int hashCode() {
    return lsn != null ? lsn.hashCode() : 0;
  }

  @Override
  public String toString() {
    return toString(null);
  }

  protected String toString(final String iToAppend) {
    final StringBuilder buffer = new StringBuilder(getClass().getName());
    buffer.append("{lsn=").append(lsn);
    if (iToAppend != null) {
      buffer.append(", ");
      buffer.append(iToAppend);
    }
    buffer.append('}');
    return buffer.toString();
  }
}
