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

package com.orientechnologies.orient.core.serialization.serializer.record.binary;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.impl.OBlob;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.record.impl.ORecordFlat;
import com.orientechnologies.orient.core.serialization.serializer.record.ORecordSerializer;
import com.orientechnologies.orient.core.sql.executor.OResult;
import java.util.Base64;

public class ORecordSerializerBinary implements ORecordSerializer {

  public static final String NAME = "ORecordSerializerBinary";
  public static final ORecordSerializerBinary INSTANCE = new ORecordSerializerBinary();
  private static final byte CURRENT_RECORD_VERSION = 1;

  private ODocumentSerializer[] serializerByVersion;
  private final byte currentSerializerVersion;

  private void init() {
    serializerByVersion = new ODocumentSerializer[2];
    serializerByVersion[0] = new ORecordSerializerBinaryV0();
    serializerByVersion[1] = new ORecordSerializerBinaryV1();
  }

  public ORecordSerializerBinary(byte serializerVersion) {
    currentSerializerVersion = serializerVersion;
    init();
  }

  public ORecordSerializerBinary() {
    currentSerializerVersion = CURRENT_RECORD_VERSION;
    init();
  }

  public int getNumberOfSupportedVersions() {
    return serializerByVersion.length;
  }

  @Override
  public int getCurrentVersion() {
    return currentSerializerVersion;
  }

  @Override
  public int getMinSupportedVersion() {
    return currentSerializerVersion;
  }

  public ODocumentSerializer getSerializer(final int iVersion) {
    return serializerByVersion[iVersion];
  }

  public ODocumentSerializer getCurrentSerializer() {
    return serializerByVersion[currentSerializerVersion];
  }

  @Override
  public String toString() {
    return NAME;
  }

  @Override
  public ORecord fromStream(final byte[] iSource, ORecord iRecord, final String[] iFields) {
    if (iSource == null || iSource.length == 0) return iRecord;
    if (iRecord == null) iRecord = new ODocument();
    else if (iRecord instanceof OBlob) {
      iRecord.fromStream(iSource);
      return iRecord;
    } else if (iRecord instanceof ORecordFlat) {
      iRecord.fromStream(iSource);
      return iRecord;
    }

    final BytesContainer container = new BytesContainer(iSource).skip(1);

    try {
      if (iFields != null && iFields.length > 0)
        serializerByVersion[iSource[0]].deserializePartial((ODocument) iRecord, container, iFields);
      else serializerByVersion[iSource[0]].deserialize((ODocument) iRecord, container);
    } catch (RuntimeException e) {
      e.printStackTrace();
      OLogManager.instance()
          .warn(
              this,
              "Error deserializing record with id %s send this data for debugging: %s ",
              iRecord.getIdentity().toString(),
              Base64.getEncoder().encodeToString(iSource));
      throw e;
    }
    return iRecord;
  }

  @Override
  public byte[] toStream(ORecord record) {
    if (record instanceof OBlob) {
      return record.toStream();
    } else if (record instanceof ORecordFlat) {
      return record.toStream();
    } else {
      ODocument documentToSerialize = (ODocument) record;

      final BytesContainer container = new BytesContainer();

      // WRITE SERIALIZER VERSION
      int pos = container.alloc(1);
      container.bytes[pos] = currentSerializerVersion;
      // SERIALIZE RECORD
      serializerByVersion[currentSerializerVersion].serialize(documentToSerialize, container);

      return container.fitBytes();
    }
  }

  @Override
  public String[] getFieldNames(ODocument reference, final byte[] iSource) {
    if (iSource == null || iSource.length == 0) return new String[0];

    final BytesContainer container = new BytesContainer(iSource).skip(1);

    try {
      return serializerByVersion[iSource[0]].getFieldNames(reference, container, false);
    } catch (RuntimeException e) {
      OLogManager.instance()
          .warn(
              this,
              "Error deserializing record to get field-names, send this data for debugging: %s ",
              Base64.getEncoder().encodeToString(iSource));
      throw e;
    }
  }

  @Override
  public boolean getSupportBinaryEvaluate() {
    return true;
  }

  @Override
  public String getName() {
    return NAME;
  }

  public OResult getBinaryResult(ODatabaseSession db, byte[] bytes, ORecordId id) {
    ODocumentSerializer serializer = getSerializer(bytes[0]);
    return new OResultBinary(db, bytes, 1, bytes.length, serializer, id);
  }
}
