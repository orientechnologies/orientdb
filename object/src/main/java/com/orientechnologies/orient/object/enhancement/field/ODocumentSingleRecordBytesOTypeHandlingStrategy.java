/*
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
package com.orientechnologies.orient.object.enhancement.field;

import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.record.impl.ORecordBytes;

/**
 * {@link ODocumentFieldOTypeHandlingStrategy} that stores each {@link OType#BINARY} object in a {@link ORecordBytes}.
 * 
 * Binary data optimization: http://orientdb.com/docs/2.2/Binary-Data.html
 * 
 * @author diegomtassis <a href="mailto:dta@compart.com">Diego Martin Tassis</a>
 */
public class ODocumentSingleRecordBytesOTypeHandlingStrategy implements ODocumentFieldOTypeHandlingStrategy {

  @Override
  public ODocument store(ODocument iRecord, String fieldName, Object fieldValue) {

    byte[] bytes = fieldValue != null ? (byte[]) fieldValue : null;
    ORecordBytes recordBytes;
    if ((recordBytes = iRecord.field(fieldName)) == null) {
      // No data yet
      recordBytes = new ORecordBytes();
      iRecord.field(fieldName, recordBytes);
    } else {
      // There's already a document storing some binary data
      recordBytes.clear();
    }

    if (bytes != null) {
      recordBytes.fromStream(bytes);
    }

    return iRecord;
  }

  @Override
  public Object load(ODocument iRecord, String fieldName) {
    ORecordBytes oRecordBytes = iRecord.field(fieldName);
    return oRecordBytes != null ? oRecordBytes.toStream() : null;
  }

  @Override
  public OType getOType() {
    return OType.BINARY;
  }
}
