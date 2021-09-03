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
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.metadata.schema.OImmutableSchema;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.impl.OBlob;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.record.impl.ORecordFlat;
import com.orientechnologies.orient.core.serialization.serializer.record.ORecordSerializer;
import java.util.Base64;

public class ORecordSerializerNetwork implements ORecordSerializer {

  public static final String NAME = "onet_ser_v0";
  public static final ORecordSerializerNetwork INSTANCE = new ORecordSerializerNetwork();
  private static final byte CURRENT_RECORD_VERSION = 0;

  private final ODocumentSerializer[] serializerByVersion;

  public ORecordSerializerNetwork() {
    serializerByVersion = new ODocumentSerializer[1];
    serializerByVersion[0] = new ORecordSerializerNetworkV0();
  }

  @Override
  public int getCurrentVersion() {
    return CURRENT_RECORD_VERSION;
  }

  @Override
  public int getMinSupportedVersion() {
    return CURRENT_RECORD_VERSION;
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

    BytesContainer container = new BytesContainer(iSource);
    container.skip(1);

    try {
      if (iFields != null && iFields.length > 0)
        serializerByVersion[iSource[0]].deserializePartial((ODocument) iRecord, container, iFields);
      else serializerByVersion[iSource[0]].deserialize((ODocument) iRecord, container);
    } catch (RuntimeException e) {
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
  public byte[] toStream(ORecord iSource) {
    if (iSource instanceof OBlob) {
      return iSource.toStream();
    } else if (iSource instanceof ORecordFlat) {
      return iSource.toStream();
    } else {
      final BytesContainer container = new BytesContainer();

      // WRITE SERIALIZER VERSION
      int pos = container.alloc(1);
      container.bytes[pos] = CURRENT_RECORD_VERSION;
      // SERIALIZE RECORD
      serializerByVersion[CURRENT_RECORD_VERSION].serialize((ODocument) iSource, container);

      return container.fitBytes();
    }
  }

  public byte[] serializeValue(Object value, OType type) {
    ODatabaseDocumentInternal db = ODatabaseRecordThreadLocal.instance().getIfDefined();
    OImmutableSchema schema = null;
    if (db != null) schema = db.getMetadata().getImmutableSchemaSnapshot();
    BytesContainer bytes = new BytesContainer();
    serializerByVersion[0].serializeValue(bytes, value, type, null, schema, null);
    return bytes.fitBytes();
  }

  public Object deserializeValue(byte[] val, OType type) {
    BytesContainer bytes = new BytesContainer(val);
    return serializerByVersion[0].deserializeValue(bytes, type, null);
  }

  @Override
  public boolean getSupportBinaryEvaluate() {
    return false;
  }

  @Override
  public String getName() {
    return NAME;
  }

  @Override
  public String[] getFieldNames(ODocument reference, byte[] iSource) {
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
}
