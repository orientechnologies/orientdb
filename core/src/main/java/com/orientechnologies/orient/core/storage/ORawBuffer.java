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
package com.orientechnologies.orient.core.storage;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.version.ORecordVersion;
import com.orientechnologies.orient.core.version.OVersionFactory;

public class ORawBuffer implements Externalizable {
  public byte[]         buffer;
  public ORecordVersion version;
  public byte           recordType;

  /**
   * Constructor used by serialization.
   */
  public ORawBuffer() {
    version = OVersionFactory.instance().createVersion();
  }

  public ORawBuffer(final byte[] buffer, final ORecordVersion version, final byte recordType) {
    this.buffer = buffer;
    this.version = version.copy();
    this.recordType = recordType;
  }

  /**
   * Creates a new object by the record received.
   * 
   * @param iRecord
   */
  public ORawBuffer(final ORecordInternal<?> iRecord) {
    this.buffer = iRecord.toStream();
    this.version = iRecord.getRecordVersion().copy();
    this.recordType = iRecord.getRecordType();
  }

  public void readExternal(final ObjectInput iInput) throws IOException, ClassNotFoundException {
    final int bufferLenght = iInput.readInt();
    if (bufferLenght > 0) {
      buffer = new byte[bufferLenght];
      for (int pos = 0, bytesReaded = 0; pos < bufferLenght; pos += bytesReaded) {
        bytesReaded = iInput.read(buffer, pos, buffer.length - pos);
      }
    } else
      buffer = null;
    version.getSerializer().readFrom(iInput, version);
    recordType = iInput.readByte();
  }

  public void writeExternal(final ObjectOutput iOutput) throws IOException {
    final int bufferLenght = buffer != null ? buffer.length : 0;
    iOutput.writeInt(bufferLenght);
    if (bufferLenght > 0)
      iOutput.write(buffer);
    version.getSerializer().writeTo(iOutput, version);
    iOutput.write(recordType);
  }

  @Override
  public String toString() {
    return "size:" + (buffer != null ? buffer.length : "empty");
  }
}
