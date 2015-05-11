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

import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.storage.ORawBuffer;
import com.orientechnologies.orient.core.storage.OStorage;
import com.orientechnologies.orient.core.storage.OStorageOperationResult;
import com.orientechnologies.orient.core.version.ORecordVersion;

/**
 * Auto merges new record with the existent. Collections are also merged, item by item.
 * 
 * @author Luca Garulli
 */
public class OAutoMergeRecordConflictStrategy extends OVersionRecordConflictStrategy {
  public static final String NAME = "automerge";

  @Override
  public byte[] onUpdate(OStorage storage, byte iRecordType, final ORecordId rid, final ORecordVersion iRecordVersion,
      final byte[] iRecordContent, final ORecordVersion iDatabaseVersion) {

    if (iRecordType == ODocument.RECORD_TYPE) {
      // No need lock, is already inside a lock.
      OStorageOperationResult<ORawBuffer> res = storage.readRecord(rid, null, false, null);
      final ODocument storedRecord = new ODocument(rid).fromStream(res.getResult().getBuffer());
      final ODocument newRecord = new ODocument(rid).fromStream(iRecordContent);

      storedRecord.merge(newRecord, false, true);

      iDatabaseVersion.setCounter(Math.max(iDatabaseVersion.getCounter(), iRecordVersion.getCounter()));

      return storedRecord.toStream();
    } else
      // NO DOCUMENT, CANNOT MERGE SO RELY TO THE VERSION CHECK
      checkVersions(rid, iRecordVersion, iDatabaseVersion);

    return null;
  }

  @Override
  public String getName() {
    return NAME;
  }
}
