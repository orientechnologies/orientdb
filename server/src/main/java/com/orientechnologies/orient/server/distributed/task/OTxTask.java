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

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.List;

import com.orientechnologies.common.concur.ONeedRetryException;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.exception.OTransactionException;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.version.ORecordVersion;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.distributed.ODistributedRequest;
import com.orientechnologies.orient.server.distributed.ODistributedResponse;
import com.orientechnologies.orient.server.distributed.ODistributedServerLog;
import com.orientechnologies.orient.server.distributed.ODistributedServerLog.DIRECTION;
import com.orientechnologies.orient.server.distributed.ODistributedServerManager;

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

      for (OAbstractRecordReplicatedTask task : tasks) {
        task.execute(iServer, iManager, database);
      }

      database.commit();

    } catch (ONeedRetryException e) {
      return Boolean.FALSE;
    } catch (OTransactionException e) {
      return Boolean.FALSE;
    } catch (Exception e) {
      OLogManager.instance().error(this, "Error on distirbuted transaction commit", e);
      return Boolean.FALSE;
    }
    return Boolean.TRUE;
  }

  @Override
  public QUORUM_TYPE getQuorumType() {
    return QUORUM_TYPE.WRITE;
  }

  @Override
  public OFixTxTask getFixTask(final ODistributedRequest iRequest, final ODistributedResponse iBadResponse,
      final ODistributedResponse iGoodResponse) {
    final OFixTxTask fixTask = new OFixTxTask();

    for (OAbstractRecordReplicatedTask t : tasks) {
      final ORecordId rid = t.getRid();

      final ORecordInternal<?> rec = rid.getRecord();
      if (rec == null)
        fixTask.add(new ODeleteRecordTask(rid, null));
      else {
        final ORecordVersion v = rec.getRecordVersion();
        v.setRollbackMode();
        fixTask.add(new OUpdateRecordTask(rid, rec.toStream(), v, rec.getRecordType()));
      }
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
}
