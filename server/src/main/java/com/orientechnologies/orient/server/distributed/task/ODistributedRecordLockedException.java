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
package com.orientechnologies.orient.server.distributed.task;

import com.orientechnologies.common.concur.ONeedRetryException;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.server.distributed.ODistributedRequestId;

/**
 * Exception thrown when a record is locked by a running distributed transaction.
 *
 * @author Luca Garulli (l.garulli--at--orientdb.com)
 */
public class ODistributedRecordLockedException extends ONeedRetryException {
  protected ORID rid;
  protected ODistributedRequestId lockHolder;
  protected String node;

  public ODistributedRecordLockedException(final ODistributedRecordLockedException exception) {
    super(exception);
  }

  public ODistributedRecordLockedException(final String localNodeName, final ORID iRid) {
    super("Cannot acquire lock on record " + iRid + " on server '" + localNodeName + "'. ");
    this.rid = iRid;
    this.node = localNodeName;
  }

  public ODistributedRecordLockedException(
      final String localNodeName,
      final ORID iRid,
      final ODistributedRequestId iLockingRequestId,
      long timeout) {
    super(
        "Timeout ("
            + timeout
            + "ms) on acquiring lock on record "
            + iRid
            + " on server '"
            + localNodeName
            + "'. It is locked by request "
            + iLockingRequestId);
    this.rid = iRid;
    this.lockHolder = iLockingRequestId;
    this.node = localNodeName;
  }

  public ORID getRid() {
    return rid;
  }

  public String getNode() {
    return node;
  }

  public ODistributedRequestId getLockHolder() {
    return lockHolder;
  }

  @Override
  public boolean equals(final Object obj) {
    if (!(obj instanceof ODistributedRecordLockedException)) return false;

    return rid.equals(((ODistributedRecordLockedException) obj).rid);
  }
}
