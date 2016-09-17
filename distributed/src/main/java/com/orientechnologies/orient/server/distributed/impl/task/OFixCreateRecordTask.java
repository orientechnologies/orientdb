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
package com.orientechnologies.orient.server.distributed.impl.task;

import com.orientechnologies.orient.core.command.OCommandDistributedReplicateRequest;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.record.ORecord;

/**
 * Fix for distributed delete record task.
 *
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 */
public class OFixCreateRecordTask extends ODeleteRecordTask {
  public static final int FACTORYID = 20;

  public OFixCreateRecordTask() {
  }

  public OFixCreateRecordTask(final ORecord record) {
    super(record);
  }

  public OFixCreateRecordTask(final ORecordId rid, final int version) {
    super(rid, version);
  }

  @Override
  public OCommandDistributedReplicateRequest.QUORUM_TYPE getQuorumType() {
    return OCommandDistributedReplicateRequest.QUORUM_TYPE.NONE;
  }

  @Override
  public String getName() {
    return "fix_record_create";
  }

  @Override
  public String toString() {
    return getName() + "(" + rid + ")";
  }

  @Override
  public int getFactoryId() {
    return FACTORYID;
  }

}
