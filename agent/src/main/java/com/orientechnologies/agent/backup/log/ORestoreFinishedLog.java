/*
 * Copyright 2015 OrientDB LTD (info(at)orientdb.com)
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 *
 *   For more information: http://www.orientdb.com
 */

package com.orientechnologies.agent.backup.log;

import com.orientechnologies.orient.core.record.impl.ODocument;

/**
 * Created by Enrico Risa on 25/03/16.
 */
public class ORestoreFinishedLog extends OBackupLog {

  public long elapsedTime = 0;

  public ORestoreFinishedLog(long unitId, long opsId, String uuid, String dbName, String mode) {
    super(unitId, opsId, uuid, dbName, mode);
  }

  @Override
  public OBackupLogType getType() {
    return OBackupLogType.RESTORE_FINISHED;
  }

  @Override
  public ODocument toDoc() {
    ODocument doc = super.toDoc();
    doc.field("elapsedTime", elapsedTime);
    return doc;
  }

  @Override
  public void fromDoc(ODocument doc) {
    super.fromDoc(doc);
    elapsedTime = doc.field("elapsedTime");
  }
}
