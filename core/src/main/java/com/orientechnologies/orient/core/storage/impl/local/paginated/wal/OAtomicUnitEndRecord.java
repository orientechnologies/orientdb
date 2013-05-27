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

import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.common.serialization.types.OLongSerializer;

/**
 * @author Andrey Lomakin
 * @since 24.05.13
 */
public class OAtomicUnitEndRecord implements OWALRecord {
  private OLogSequenceNumber lsn;

  private OLogSequenceNumber prevUnitRecord;

  public OAtomicUnitEndRecord() {
  }

  public OAtomicUnitEndRecord(OLogSequenceNumber prevUnitRecord) {
    assert prevUnitRecord != null;
    this.prevUnitRecord = prevUnitRecord;
  }

  public OLogSequenceNumber getPrevUnitRecord() {
    return prevUnitRecord;
  }

  @Override
  public int toStream(byte[] content, int offset) {
    OIntegerSerializer.INSTANCE.serializeNative(prevUnitRecord.getSegment(), content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    OLongSerializer.INSTANCE.serializeNative(prevUnitRecord.getPosition(), content, offset);
    offset += OLongSerializer.LONG_SIZE;

    return offset;
  }

  @Override
  public int fromStream(byte[] content, int offset) {
    int segment = OIntegerSerializer.INSTANCE.deserializeNative(content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    long position = OLongSerializer.INSTANCE.deserializeNative(content, offset);
    offset += OLongSerializer.LONG_SIZE;

    prevUnitRecord = new OLogSequenceNumber(segment, position);
    return offset;
  }

  @Override
  public int serializedSize() {
    return OLongSerializer.LONG_SIZE + OIntegerSerializer.INT_SIZE;
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
  public boolean equals(Object o) {
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;

    OAtomicUnitEndRecord that = (OAtomicUnitEndRecord) o;

    if (!prevUnitRecord.equals(that.prevUnitRecord))
      return false;

    return true;
  }

  @Override
  public int hashCode() {
    return prevUnitRecord.hashCode();
  }
}
