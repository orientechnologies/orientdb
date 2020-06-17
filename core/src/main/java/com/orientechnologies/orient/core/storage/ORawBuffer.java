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

import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.type.OBuffer;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

public class ORawBuffer extends OBuffer {
  public int version;
  public byte recordType;

  /** Constructor used by serialization. */
  public ORawBuffer() {
    version = 0;
  }

  public ORawBuffer(final byte[] buffer, final int version, final byte recordType) {
    this.buffer = buffer;
    this.version = version;
    this.recordType = recordType;
  }

  /** Creates a new object by the record received. */
  public ORawBuffer(final ORecord iRecord) {
    this.buffer = iRecord.toStream();
    this.version = iRecord.getVersion();
    this.recordType = ORecordInternal.getRecordType(iRecord);
  }

  @Override
  public void readExternal(final ObjectInput iInput) throws IOException, ClassNotFoundException {
    super.readExternal(iInput);
    version = iInput.readInt();
    recordType = iInput.readByte();
  }

  @Override
  public void writeExternal(final ObjectOutput iOutput) throws IOException {
    super.writeExternal(iOutput);
    iOutput.writeInt(version);
    iOutput.writeByte(recordType);
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    if (!super.equals(o)) return false;

    ORawBuffer that = (ORawBuffer) o;

    if (recordType != that.recordType) return false;
    return version == that.version;
  }

  @Override
  public int hashCode() {
    int result = super.hashCode();
    result = 31 * result + version;
    result = 31 * result + (int) recordType;
    return result;
  }
}
