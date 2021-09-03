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

package com.orientechnologies.orient.core.conflict;

import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.serialization.serializer.record.ORecordSaveThreadLocal;
import com.orientechnologies.orient.core.storage.ORawBuffer;
import com.orientechnologies.orient.core.storage.OStorage;
import com.orientechnologies.orient.core.storage.OStorageOperationResult;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Auto merges new record with the existent. Collections are also merged, item by item.
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 */
public class OAutoMergeRecordConflictStrategy extends OVersionRecordConflictStrategy {
  public static final String NAME = "automerge";

  @Override
  public byte[] onUpdate(
      OStorage storage,
      byte iRecordType,
      final ORecordId rid,
      final int iRecordVersion,
      final byte[] iRecordContent,
      final AtomicInteger iDatabaseVersion) {

    if (iRecordType == ODocument.RECORD_TYPE) {
      // No need lock, is already inside a lock. Use database to read temporary objects too
      OStorageOperationResult<ORawBuffer> res = storage.readRecord(rid, null, false, false, null);
      final ODocument storedRecord = new ODocument(rid).fromStream(res.getResult().getBuffer());

      ODocument newRecord = (ODocument) ORecordSaveThreadLocal.getLast();
      if (newRecord == null || !newRecord.getIdentity().equals(rid))
        newRecord = new ODocument(rid).fromStream(iRecordContent);

      storedRecord.merge(newRecord, true, true);

      iDatabaseVersion.set(Math.max(iDatabaseVersion.get(), iRecordVersion) + 1);

      return storedRecord.toStream();
    } else
      // NO DOCUMENT, CANNOT MERGE SO RELY TO THE VERSION CHECK
      checkVersions(rid, iRecordVersion, iDatabaseVersion.get());

    return null;
  }

  @Override
  public String getName() {
    return NAME;
  }
}
