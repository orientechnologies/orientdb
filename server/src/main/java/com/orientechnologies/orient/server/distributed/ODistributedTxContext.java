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
package com.orientechnologies.orient.server.distributed;

import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.server.distributed.task.ORemoteTask;

import java.util.List;

/**
 * Represent a distributed transaction context.
 *
 * @author Luca Garulli
 */
public interface ODistributedTxContext {
  void lock(ORID rid);

  void lock(ORID rid, long timeout);

  void addUndoTask(ORemoteTask undoTask);

  ODistributedRequestId getReqId();

  void commit();

  void fix(ODatabaseDocumentInternal database, List<ORemoteTask> fixTasks);

  int rollback(ODatabaseDocumentInternal database);

  void destroy();

  void unlock();

  long getStartedOn();
}