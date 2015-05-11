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

package com.orientechnologies.orient.core.conflict;

import java.util.Arrays;

import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.record.ORecordAbstract;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.record.impl.ODocumentHelper;
import com.orientechnologies.orient.core.storage.ORawBuffer;
import com.orientechnologies.orient.core.storage.OStorage;
import com.orientechnologies.orient.core.storage.OStorageOperationResult;
import com.orientechnologies.orient.core.version.ORecordVersion;

/**
 * Record conflict strategy that check the records content: if content is the same, se the higher version number.
 * 
 * @author Luca Garulli
 */
public class OContentRecordConflictStrategy extends OVersionRecordConflictStrategy {
  public static final String NAME = "content";

  @Override
  public byte[] onUpdate(OStorage storage, final byte iRecordType, final ORecordId rid, final ORecordVersion iRecordVersion,
      final byte[] iRecordContent, final ORecordVersion iDatabaseVersion) {

    final boolean hasSameContent;

    if (iRecordType == ODocument.RECORD_TYPE) {
      // No need lock, is already inside a lock.
      OStorageOperationResult<ORawBuffer> res = storage.readRecord(rid, null, false, null);
      final ODocument storedRecord = new ODocument(rid).fromStream(res.getResult().getBuffer());
      final ODocument newRecord = new ODocument().fromStream(iRecordContent);

      final ODatabaseDocumentInternal currentDb = ODatabaseRecordThreadLocal.INSTANCE.get();
      hasSameContent = ODocumentHelper.hasSameContentOf(storedRecord, currentDb, newRecord, currentDb, null, false);
    } else {
      // CHECK BYTE PER BYTE
      final ORecordAbstract storedRecord = rid.getRecord();
      hasSameContent = Arrays.equals(storedRecord.toStream(), iRecordContent);
    }

    if (hasSameContent)
      // OK
      iDatabaseVersion.setCounter(Math.max(iDatabaseVersion.getCounter(), iRecordVersion.getCounter()));
    else
      // NO DOCUMENT, CANNOT MERGE SO RELY TO THE VERSION CHECK
      checkVersions(rid, iRecordVersion, iDatabaseVersion);

    return null;
  }

  @Override
  public String getName() {
    return NAME;
  }
}
