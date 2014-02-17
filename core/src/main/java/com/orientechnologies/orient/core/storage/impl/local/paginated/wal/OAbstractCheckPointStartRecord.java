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
  private OLogSequenceNumber previousCheckpoint;

  protected OAbstractCheckPointStartRecord() {
  }

  protected OAbstractCheckPointStartRecord(OLogSequenceNumber previousCheckpoint) {
    this.previousCheckpoint = previousCheckpoint;
  }

  public OLogSequenceNumber getPreviousCheckpoint() {
    return previousCheckpoint;
  }

  @Override
  public int toStream(byte[] content, int offset) {
    if (previousCheckpoint == null) {
      content[offset] = 0;
      offset++;
      return offset;
    }

    content[offset] = 1;
    offset++;

    OLongSerializer.INSTANCE.serializeNative(previousCheckpoint.getSegment(), content, offset);
    offset += OLongSerializer.LONG_SIZE;

    OLongSerializer.INSTANCE.serializeNative(previousCheckpoint.getPosition(), content, offset);
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

    previousCheckpoint = new OLogSequenceNumber(segment, position);

    return offset;
  }

  @Override
  public int serializedSize() {
    if (previousCheckpoint == null)
      return 1;

    return 2 * OLongSerializer.LONG_SIZE + 1;
  }

  @Override
  public boolean isUpdateMasterRecord() {
    return true;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;

    OAbstractCheckPointStartRecord that = (OAbstractCheckPointStartRecord) o;

    if (previousCheckpoint != null ? !previousCheckpoint.equals(that.previousCheckpoint) : that.previousCheckpoint != null)
      return false;

    return true;
  }

  @Override
  public int hashCode() {
    return previousCheckpoint != null ? previousCheckpoint.hashCode() : 0;
  }

  @Override
  public String toString() {
    return toString("previousCheckpoint=" + previousCheckpoint);
  }
}
