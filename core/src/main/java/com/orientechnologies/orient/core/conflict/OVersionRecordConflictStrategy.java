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

import com.orientechnologies.orient.core.db.record.ORecordOperation;
import com.orientechnologies.orient.core.exception.OConcurrentModificationException;
import com.orientechnologies.orient.core.exception.OFastConcurrentModificationException;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.storage.OStorage;
import com.orientechnologies.orient.core.version.ORecordVersion;

/**
 * Default strategy that checks the record version number: if the current update has a version different than stored one, then a
 * OConcurrentModificationException is thrown.
 * 
 * @author Luca Garulli
 */
public class OVersionRecordConflictStrategy implements ORecordConflictStrategy {
  public static final String NAME = "version";

  @Override
  public byte[] onUpdate(OStorage storage, final byte iRecordType, final ORecordId rid,
      final ORecordVersion iRecordVersion, final byte[] iRecordContent, final ORecordVersion iDatabaseVersion) {
    checkVersions(rid, iRecordVersion, iDatabaseVersion);
    return null;
  }

  @Override
  public String getName() {
    return NAME;
  }

  protected void checkVersions(final ORecordId rid, final ORecordVersion iRecordVersion, final ORecordVersion iDatabaseVersion) {
    if (OFastConcurrentModificationException.enabled())
      throw OFastConcurrentModificationException.instance();
    else
      throw new OConcurrentModificationException(rid, iDatabaseVersion, iRecordVersion, ORecordOperation.UPDATED);
  }
}
