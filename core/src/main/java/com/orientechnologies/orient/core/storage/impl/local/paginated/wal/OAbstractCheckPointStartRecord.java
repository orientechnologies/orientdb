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

import com.orientechnologies.common.serialization.types.OLongSerializer;

/**
 * @author Andrey Lomakin
 * @since 14.05.13
 */
public abstract class OAbstractCheckPointStartRecord extends OAbstractWALRecord {
  protected OAbstractCheckPointStartRecord() {
  }

  protected OAbstractCheckPointStartRecord(final OLogSequenceNumber lsn) {
    super(lsn);
  }

  @Override
  public int toStream(final byte[] content, int offset) {
    if (lsn == null) {
      content[offset] = 0;
      offset++;
      return offset;
    }

    content[offset] = 1;
    offset++;

    OLongSerializer.INSTANCE.serializeNative(lsn.getSegment(), content, offset);
    offset += OLongSerializer.LONG_SIZE;

    OLongSerializer.INSTANCE.serializeNative(lsn.getPosition(), content, offset);
    offset += OLongSerializer.LONG_SIZE;

    return offset;
  }

  @Override
  public int fromStream(byte[] content, int offset) {
    if (content[offset] == 0) {
      offset++;
      return offset;
    }

    offset++;

    long segment = OLongSerializer.INSTANCE.deserializeNative(content, offset);
    offset += OLongSerializer.LONG_SIZE;

    long position = OLongSerializer.INSTANCE.deserializeNative(content, offset);
    offset += OLongSerializer.LONG_SIZE;

    lsn = new OLogSequenceNumber(segment, position);

    return offset;
  }

  @Override
  public int serializedSize() {
    if (lsn == null)
      return 1;

    return 2 * OLongSerializer.LONG_SIZE + 1;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;
    return true;
  }

  @Override
  public boolean isUpdateMasterRecord() {
    return true;
  }
}
