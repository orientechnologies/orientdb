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

import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.record.ORecordAbstract;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.record.impl.ODocumentHelper;
import com.orientechnologies.orient.core.storage.ORawBuffer;
import com.orientechnologies.orient.core.storage.OStorage;
import com.orientechnologies.orient.core.storage.OStorageOperationResult;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Record conflict strategy that check the records content: if content is the same, se the higher
 * version number.
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 */
public class OContentRecordConflictStrategy extends OVersionRecordConflictStrategy {
  public static final String NAME = "content";

  @Override
  public byte[] onUpdate(
      OStorage storage,
      final byte iRecordType,
      final ORecordId rid,
      final int iRecordVersion,
      final byte[] iRecordContent,
      final AtomicInteger iDatabaseVersion) {

    final boolean hasSameContent;

    if (iRecordType == ODocument.RECORD_TYPE) {
      // No need lock, is already inside a lock.
      OStorageOperationResult<ORawBuffer> res = storage.readRecord(rid, null, false, false, null);
      final ODocument storedRecord = new ODocument(rid).fromStream(res.getResult().getBuffer());
      final ODocument newRecord = new ODocument().fromStream(iRecordContent);

      final ODatabaseDocumentInternal currentDb = ODatabaseRecordThreadLocal.instance().get();
      hasSameContent =
          ODocumentHelper.hasSameContentOf(
              storedRecord, currentDb, newRecord, currentDb, null, false);
    } else {
      // CHECK BYTE PER BYTE
      final ORecordAbstract storedRecord = rid.getRecord();
      hasSameContent = Arrays.equals(storedRecord.toStream(), iRecordContent);
    }

    if (hasSameContent)
      // OK
      iDatabaseVersion.set(Math.max(iDatabaseVersion.get(), iRecordVersion));
    else
      // NO DOCUMENT, CANNOT MERGE SO RELY TO THE VERSION CHECK
      checkVersions(rid, iRecordVersion, iDatabaseVersion.get());

    return null;
  }

  @Override
  public String getName() {
    return NAME;
  }
}
