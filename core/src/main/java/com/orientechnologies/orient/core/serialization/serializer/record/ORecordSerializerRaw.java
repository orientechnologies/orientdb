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
package com.orientechnologies.orient.core.serialization.serializer.record;

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.exception.OSerializationException;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.impl.OBlob;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.record.impl.ORecordBytes;

public class ORecordSerializerRaw implements ORecordSerializer {
  public static final String NAME = "ORecordDocumentRaw";

  public ORecord fromStream(final byte[] iSource) {
    return new ORecordBytes(iSource);
  }

  @Override
  public int getCurrentVersion() {
    return 0;
  }

  @Override
  public int getMinSupportedVersion() {
    return 0;
  }

  @Override
  public String[] getFieldNames(ODocument reference, byte[] iSource) {
    return null;
  }

  @Override
  public String toString() {
    return NAME;
  }

  @Override
  public ORecord fromStream(final byte[] iSource, final ORecord iRecord, String[] iFields) {
    final OBlob record = (OBlob) iRecord;
    record.reset();
    record.fromStream(iSource);

    return record;
  }

  @Override
  public byte[] toStream(final ORecord iSource) {
    try {
      return iSource.toStream();
    } catch (Exception e) {
      final String message =
          "Error on unmarshalling object in binary format: " + iSource.getIdentity();
      OLogManager.instance().error(this, message, e);
      throw OException.wrapException(new OSerializationException(message), e);
    }
  }

  @Override
  public boolean getSupportBinaryEvaluate() {
    return false;
  }

  @Override
  public String getName() {
    return NAME;
  }
}
