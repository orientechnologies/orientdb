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
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.version.ORecordVersion;
import com.orientechnologies.orient.core.version.OVersionFactory;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.distributed.ODistributedServerLog;
import com.orientechnologies.orient.server.distributed.ODistributedServerLog.DIRECTION;
import com.orientechnologies.orient.server.distributed.ODistributedServerManager;
import com.orientechnologies.orient.server.distributed.ODistributedStorage;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
  * Distributed task to fix delete record in conflict on synchronization.
  *
  * @author Luca Garulli (l.garulli--at--orientechnologies.com)
  *
  */
 public class OResurrectRecordTask extends OAbstractRemoteTask {
   private static final long serialVersionUID = 1L;
   private ORecordId         rid;
   private ORecordVersion    version;

   public OResurrectRecordTask() {
   }

   public OResurrectRecordTask(final ORecordId iRid, final ORecordVersion iVersion) {
     rid = iRid;
     version = iVersion;
   }

   public ORecordId getRid() {
     return rid;
   }

   public ORecordVersion getVersion() {
     return version;
   }

   @Override
   public Object execute(final OServer iServer, ODistributedServerManager iManager, final ODatabaseDocumentTx database)
       throws Exception {
     ODistributedServerLog.debug(this, iManager.getLocalNodeName(), getNodeSource(), DIRECTION.IN,
         "resurrecting deleted record %s/%s v.%s", database.getName(), rid.toString(), version.toString());

     if (((ODistributedStorage) database.getStorage()).resurrectDeletedRecord(rid)) {
       ODistributedServerLog.debug(this, iManager.getLocalNodeName(), getNodeSource(), DIRECTION.IN,
           "+-> resurrected deleted record");
       return Boolean.TRUE;
     }

     ODistributedServerLog.error(this, iManager.getLocalNodeName(), getNodeSource(), DIRECTION.IN,
         "+-> error on resurrecting deleted record: the record is already deleted");
     return Boolean.FALSE;
   }

   public OCommandDistributedReplicateRequest.QUORUM_TYPE getQuorumType() {
     return OCommandDistributedReplicateRequest.QUORUM_TYPE.NONE;
   }

   @Override
   public void writeExternal(final ObjectOutput out) throws IOException {
     out.writeUTF(rid.toString());
     if (version == null)
       version = OVersionFactory.instance().createUntrackedVersion();
     version.getSerializer().writeTo(out, version);
   }

   @Override
   public void readExternal(final ObjectInput in) throws IOException, ClassNotFoundException {
     rid = new ORecordId(in.readUTF());
     final int contentSize = in.readInt();
     if (version == null)
       version = OVersionFactory.instance().createUntrackedVersion();
     version.getSerializer().readFrom(in, version);
   }

   @Override
   public String getName() {
     return "fix_record_delete";
   }
 }
