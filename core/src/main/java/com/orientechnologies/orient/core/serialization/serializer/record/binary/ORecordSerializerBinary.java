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

package com.orientechnologies.orient.core.serialization.serializer.record.binary;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.serialization.OBase64Utils;
import com.orientechnologies.orient.core.serialization.serializer.record.ORecordSerializer;
import com.orientechnologies.orient.core.serialization.serializer.record.OSerializationSetThreadLocal;

public class ORecordSerializerBinary implements ORecordSerializer {

  public static final String                  NAME                   = "ORecordSerializerBinary";
  public static final ORecordSerializerBinary INSTANCE               = new ORecordSerializerBinary();
  private static final byte                   CURRENT_RECORD_VERSION = 0;

  private ODocumentSerializer[]               serializerByVersion;

  public ORecordSerializerBinary() {
    serializerByVersion = new ODocumentSerializer[1];
    serializerByVersion[0] = new ORecordSerializerBinaryV0();
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
    if (iSource.length == 0)
      return iRecord;
    if (iRecord == null)
      iRecord = new ODocument();
    else
      checkTypeODocument(iRecord);

    BytesContainer container = new BytesContainer(iSource);
    container.skip(1);

    try {
      if (iFields != null && iFields.length > 0)
        serializerByVersion[iSource[0]].deserializePartial((ODocument) iRecord, container, iFields);
      else
        serializerByVersion[iSource[0]].deserialize((ODocument) iRecord, container);
    } catch (RuntimeException e) {
      OLogManager.instance().warn(this, "Error deserializing record with id %s send this data for debugging: %s ",
          iRecord.getIdentity().toString(), OBase64Utils.encodeBytes(iSource));
      throw e;
    }
    return iRecord;
  }

  @Override
  public byte[] toStream(final ORecord iSource, final boolean iOnlyDelta) {
    checkTypeODocument(iSource);

    final BytesContainer container = new BytesContainer();

    // WRITE SERIALIZER VERSION
    int pos = container.alloc(1);
    container.bytes[pos] = CURRENT_RECORD_VERSION;

    if (!OSerializationSetThreadLocal.checkAndAdd((ODocument) iSource)) {
      // SERIALIZE CLASS ONLY
      serializerByVersion[CURRENT_RECORD_VERSION].serialize((ODocument) iSource, container, true);

      // SET SERIALIZATION AS PARTIAL
      OSerializationSetThreadLocal.setPartial((ODocument) iSource);

      return container.fitBytes();
    }

    try {
      // SERIALIZE RECORD
      serializerByVersion[CURRENT_RECORD_VERSION].serialize((ODocument) iSource, container, false);
    } finally {
      OSerializationSetThreadLocal.removeCheck((ODocument) iSource);
    }
    
    return container.fitBytes();
  }

  private void checkTypeODocument(final ORecord iRecord) {
    if (!(iRecord instanceof ODocument)) {
      throw new UnsupportedOperationException("The " + ORecordSerializerBinary.NAME + " don't support record of type "
          + iRecord.getClass().getName());
    }
  }

}
