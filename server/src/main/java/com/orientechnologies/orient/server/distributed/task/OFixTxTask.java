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
package com.orientechnologies.orient.server.distributed.task;

import com.orientechnologies.orient.core.command.OCommandDistributedReplicateRequest;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.server.OServer;
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
 public class OFixTxTask extends OAbstractRemoteTask {
   private static final long         serialVersionUID = 1L;
   private List<OAbstractRemoteTask> tasks            = new ArrayList<OAbstractRemoteTask>();

   public OFixTxTask() {
   }

   public List<OAbstractRemoteTask> getTasks() {
     return tasks;
   }

   public void add(final OAbstractRemoteTask iTask) {
     tasks.add(iTask);
   }

   @Override
   public Object execute(final OServer iServer, ODistributedServerManager iManager, final ODatabaseDocumentTx database)
       throws Exception {
     ODistributedServerLog.debug(this, iManager.getLocalNodeName(), getNodeSource(), DIRECTION.IN,
         "fixing %d conflicts found during committing transaction against db=%s...", tasks.size(), database.getName());

     ODatabaseRecordThreadLocal.INSTANCE.set(database);
     try {

       for (OAbstractRemoteTask task : tasks) {
         task.execute(iServer, iManager, database);
       }

     } catch (Exception e) {
       return Boolean.FALSE;
     }

     return Boolean.TRUE;
   }

   @Override
   public OCommandDistributedReplicateRequest.QUORUM_TYPE getQuorumType() {
     return OCommandDistributedReplicateRequest.QUORUM_TYPE.NONE;
   }

   @Override
   public void writeExternal(final ObjectOutput out) throws IOException {
     out.writeInt(tasks.size());
     for (OAbstractRemoteTask task : tasks)
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
     return "fix_tx";
   }
 //
 //  @Override
 //  public String getQueueName(final String iOriginalQueueName) {
 //    return iOriginalQueueName + OCreateRecordTask.SUFFIX_QUEUE_NAME;
 //  }
 }
