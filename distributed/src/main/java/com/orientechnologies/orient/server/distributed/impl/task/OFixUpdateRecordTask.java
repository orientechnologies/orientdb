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
package com.orientechnologies.orient.server.distributed.impl.task;

import com.orientechnologies.orient.core.command.OCommandDistributedReplicateRequest;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.record.ORecord;

/**
 * Fix a distributed updated.
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 *
 */
public class OFixUpdateRecordTask extends OUpdateRecordTask {
  public static final int FACTORYID = 21;

  public OFixUpdateRecordTask() {
  }

  public OFixUpdateRecordTask(final ORecord iRecord, final int version) {
    super(iRecord, version);
  }

  public OFixUpdateRecordTask(final ORecordId iRecordId, final byte[] iContent, final int iVersion, final byte iRecordType) {
    super(iRecordId, iContent, iVersion, iRecordType);
  }

  @Override
  public OCommandDistributedReplicateRequest.QUORUM_TYPE getQuorumType() {
    return OCommandDistributedReplicateRequest.QUORUM_TYPE.NONE;
  }

  @Override
  public String getName() {
    return "fix_record_update";
  }

}
