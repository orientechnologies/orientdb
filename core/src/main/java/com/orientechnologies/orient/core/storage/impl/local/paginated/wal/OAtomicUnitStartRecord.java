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

import com.orientechnologies.common.serialization.types.OByteSerializer;
import java.nio.ByteBuffer;

/**
 * @author Andrey Lomakin (a.lomakin-at-orientdb.com)
 * @since 24.05.13
 */
public class OAtomicUnitStartRecord extends OOperationUnitRecord {
  private boolean isRollbackSupported;

  public OAtomicUnitStartRecord() {}

  public OAtomicUnitStartRecord(final boolean isRollbackSupported, final long unitId) {
    super(unitId);
    this.isRollbackSupported = isRollbackSupported;
  }

  public boolean isRollbackSupported() {
    return isRollbackSupported;
  }

  @Override
  protected void serializeToByteBuffer(ByteBuffer buffer) {
    buffer.put(isRollbackSupported ? (byte) 1 : 0);
  }

  @Override
  protected void deserializeFromByteBuffer(ByteBuffer buffer) {
    isRollbackSupported = buffer.get() > 0;
  }

  @Override
  public int serializedSize() {
    return super.serializedSize() + OByteSerializer.BYTE_SIZE;
  }

  @Override
  public int getId() {
    return WALRecordTypes.ATOMIC_UNIT_START_RECORD;
  }

  @Override
  public String toString() {
    return toString("isRollbackSupported=" + isRollbackSupported);
  }
}
