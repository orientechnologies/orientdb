/*
 * Copyright 2010-2012 Luca Garulli (l.garulli--at--orientechnologies.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.orientechnologies.orient.server.distributed.task;

import com.orientechnologies.common.concur.ONeedRetryException;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.db.record.OPlaceholder;
import com.orientechnologies.orient.core.exception.OTransactionException;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.distributed.ODistributedRequest;
import com.orientechnologies.orient.server.distributed.ODistributedServerLog;
import com.orientechnologies.orient.server.distributed.ODistributedServerLog.DIRECTION;
import com.orientechnologies.orient.server.distributed.ODistributedServerManager;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.List;

/**
 * Distributed create record task used for synchronization.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
public class OTxTask extends OAbstractReplicatedTask {
  private static final long                   serialVersionUID = 1L;

  private List<OAbstractRecordReplicatedTask> tasks            = new ArrayList<OAbstractRecordReplicatedTask>();

  public OTxTask() {
  }

  public void add(final OAbstractRecordReplicatedTask iTask) {
    tasks.add(iTask);
  }

  @Override
  public Object execute(final OServer iServer, ODistributedServerManager iManager, final ODatabaseDocumentTx database)
      throws Exception {
    ODistributedServerLog.debug(this, iManager.getLocalNodeName(), getNodeSource(), DIRECTION.IN,
        "committing transaction against db=%s...", database.getName());

    ODatabaseRecordThreadLocal.INSTANCE.set(database);

    try {
      database.begin();

      final List<Object> results = new ArrayList<Object>();

      for (OAbstractRecordReplicatedTask task : tasks)
        results.add(task.execute(iServer, iManager, database));

      database.commit();

      // SEND BACK CHANGED VALUE TO UPDATE
      for (int i = 0; i < results.size(); ++i) {
        final Object o = results.get(i);

        final OAbstractRecordReplicatedTask task = tasks.get(i);

        if (task instanceof OCreateRecordTask) {
          // SEND RID + VERSION
          final OCreateRecordTask t = (OCreateRecordTask) task;
          results.set(i, new OPlaceholder(task.getRid(), task.getVersion()));

        } else if (task instanceof OUpdateRecordTask) {
          // SEND VERSION ONLY
          final OUpdateRecordTask t = (OUpdateRecordTask) task;
          results.set(i, task.getVersion());

        } else if (task instanceof ODeleteRecordTask) {

        }
      }

      return results;

    } catch (ONeedRetryException e) {
      return e;
    } catch (OTransactionException e) {
      return e;
    } catch (Exception e) {
      OLogManager.instance().error(this, "Error on distributed transaction commit", e);
      return e;
    }
  }

  @Override
  public QUORUM_TYPE getQuorumType() {
    return QUORUM_TYPE.WRITE;
  }

  @Override
  public OFixTxTask getFixTask(final ODistributedRequest iRequest, final Object iBadResponse, final Object iGoodResponse) {
    if (!(iBadResponse instanceof List)) {
      // TODO: MANAGE ERROR ON LOCAL NODE
      ODistributedServerLog.debug(this, getNodeSource(), null, DIRECTION.NONE,
          "error on creating fix-task for request: '%s' because bad response is not expected type: %s", iRequest, iBadResponse);
      return null;
    }

    if (!(iGoodResponse instanceof List)) {
      // TODO: MANAGE ERROR ON LOCAL NODE
      ODistributedServerLog.debug(this, getNodeSource(), null, DIRECTION.NONE,
          "error on creating fix-task for request: '%s' because good response is not expected type: %s", iRequest, iBadResponse);
      return null;
    }

    final OFixTxTask fixTask = new OFixTxTask();

    for (int i = 0; i < tasks.size(); ++i) {
      final OAbstractRecordReplicatedTask t = tasks.get(i);
      OAbstractRemoteTask task = t
          .getFixTask(iRequest, ((List<Object>) iBadResponse).get(i), ((List<Object>) iGoodResponse).get(i));

      if (task != null)
        fixTask.add(task);
    }
    return fixTask;
  }

  @Override
  public OAbstractRemoteTask getUndoTask(final ODistributedRequest iRequest, final Object iBadResponse) {
    if (!(iBadResponse instanceof List)) {
      // TODO: MANAGE ERROR ON LOCAL NODE!
      ODistributedServerLog.debug(this, getNodeSource(), null, DIRECTION.NONE,
          "error on creating undo-task for request: '%s' because bad response is not expected type: %s", iRequest, iBadResponse);
      return null;
    }

    final OFixTxTask fixTask = new OFixTxTask();

    for (int i = 0; i < tasks.size(); ++i) {
      final OAbstractRecordReplicatedTask t = tasks.get(i);
      fixTask.add(t.getUndoTask(iRequest, ((List<Object>) iBadResponse).get(i)));
    }
    return fixTask;
  }

  @Override
  public void writeExternal(final ObjectOutput out) throws IOException {
    out.writeInt(tasks.size());
    for (OAbstractRecordReplicatedTask task : tasks)
      out.writeObject(task);
  }

  @Override
  public void readExternal(final ObjectInput in) throws IOException, ClassNotFoundException {
    final int size = in.readInt();
    for (int i = 0; i < size; ++i)
      tasks.add((OAbstractRecordReplicatedTask) in.readObject());
  }

  @Override
  public String getName() {
    return "tx";
  }

  @Override
  public String getPayload() {
    return null;
  }

  public List<OAbstractRecordReplicatedTask> getTasks() {
    return tasks;
  }
}
