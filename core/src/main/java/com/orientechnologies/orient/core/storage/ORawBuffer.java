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
package com.orientechnologies.orient.core.storage;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.type.OBuffer;
import com.orientechnologies.orient.core.version.ORecordVersion;
import com.orientechnologies.orient.core.version.OVersionFactory;

public class ORawBuffer extends OBuffer {
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
  public ORawBuffer(final ORecord iRecord) {
    this.buffer = iRecord.toStream();
    this.version = iRecord.getRecordVersion().copy();
    this.recordType = ORecordInternal.getRecordType(iRecord);
  }

  public void readExternal(final ObjectInput iInput) throws IOException, ClassNotFoundException {
    super.readExternal(iInput);
    version.getSerializer().readFrom(iInput, version);
    recordType = iInput.readByte();
  }

  public void writeExternal(final ObjectOutput iOutput) throws IOException {
    super.writeExternal(iOutput);
    version.getSerializer().writeTo(iOutput, version);
    iOutput.write(recordType);
  }
}
