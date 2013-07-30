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

package com.orientechnologies.orient.core.storage.impl.local.paginated.wal;

import com.orientechnologies.common.serialization.types.OByteSerializer;

/**
 * @author Andrey Lomakin
 * @since 24.05.13
 */
public class OAtomicUnitStartRecord extends OOperationUnitRecord implements OWALRecord {
  private OLogSequenceNumber lsn;

  private boolean            isRollbackSupported;

  public OAtomicUnitStartRecord() {
  }

  public OAtomicUnitStartRecord(boolean isRollbackSupported, OOperationUnitId unitId) {
    super(unitId);
    this.isRollbackSupported = isRollbackSupported;
  }

  public boolean isRollbackSupported() {
    return isRollbackSupported;
  }

  @Override
  public int toStream(byte[] content, int offset) {
    offset = super.toStream(content, offset);

    content[offset] = isRollbackSupported ? (byte) 1 : 0;
    offset++;

    return offset;

  }

  @Override
  public int fromStream(byte[] content, int offset) {
    offset = super.fromStream(content, offset);

    isRollbackSupported = content[offset] > 0;
    offset++;

    return offset;
  }

  @Override
  public int serializedSize() {
    return super.serializedSize() + OByteSerializer.BYTE_SIZE;
  }

  @Override
  public boolean isUpdateMasterRecord() {
    return false;
  }

  @Override
  public OLogSequenceNumber getLsn() {
    return lsn;
  }

  @Override
  public void setLsn(OLogSequenceNumber lsn) {
    this.lsn = lsn;
  }

  @Override
  public String toString() {
    return "OAtomicUnitStartRecord{" + "lsn=" + lsn + ", isRollbackSupported=" + isRollbackSupported + '}';
  }
}
