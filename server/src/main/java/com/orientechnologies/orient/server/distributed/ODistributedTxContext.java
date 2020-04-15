/*
 *
 *  *  Copyright 2016 Orient Technologies LTD (info(at)orientdb.com)
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
 *  * For more information: http://www.orientdb.com
 *
 */
package com.orientechnologies.orient.server.distributed;

import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.tx.OTransactionId;
import com.orientechnologies.orient.core.tx.OTransactionInternal;
import com.orientechnologies.orient.server.distributed.task.ORemoteTask;

import java.util.List;
import java.util.Set;

/**
 * Represent a distributed transaction context.
 *
 * @author Luca Garulli
 */
public interface ODistributedTxContext {
  void lock(ORID rid);

  void lock(ORID rid, long timeout);

  void lockIndexKey(Object rid);

  void addUndoTask(ORemoteTask undoTask);

  ODistributedRequestId getReqId();

  void commit(ODatabaseDocumentInternal database);

  void fix(ODatabaseDocumentInternal database, List<ORemoteTask> fixTasks);

  Set<ORecordId> rollback(ODatabaseDocumentInternal database);

  void destroy();

  void clearUndo();

  void unlock();

  long getStartedOn();

  Set<ORecordId> cancel(ODistributedServerManager current, ODatabaseDocumentInternal database);

  boolean isCanceled();

  OTransactionInternal getTransaction();

  OTransactionId getTransactionId();

  void begin(ODatabaseDocumentInternal distributed, boolean local);
}