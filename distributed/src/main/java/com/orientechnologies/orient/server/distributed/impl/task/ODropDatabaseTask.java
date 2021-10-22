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
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.distributed.ODistributedRequestId;
import com.orientechnologies.orient.server.distributed.ODistributedServerLog;
import com.orientechnologies.orient.server.distributed.ODistributedServerManager;
import com.orientechnologies.orient.server.distributed.task.OAbstractRemoteTask;

/**
 * Distributed task to drop a database on all the servers.
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 */
public class ODropDatabaseTask extends OAbstractRemoteTask {
  private static final long serialVersionUID = 1L;
  public static final int FACTORYID = 23;

  public ODropDatabaseTask() {}

  @Override
  public Object execute(
      ODistributedRequestId requestId,
      final OServer iServer,
      final ODistributedServerManager iManager,
      final ODatabaseDocumentInternal database)
      throws Exception {

    if (database == null) {
      ODistributedServerLog.warn(
          this,
          iManager.getLocalNodeName(),
          getNodeSource(),
          ODistributedServerLog.DIRECTION.IN,
          "Cannot drop database because not existent");
      return true;
    }

    ODistributedServerLog.warn(
        this,
        iManager.getLocalNodeName(),
        getNodeSource(),
        ODistributedServerLog.DIRECTION.IN,
        "Dropping database %s...",
        database.getName());
    iServer.getDatabases().internalDrop(database.getName());

    return true;
  }

  @Override
  public OCommandDistributedReplicateRequest.QUORUM_TYPE getQuorumType() {
    return OCommandDistributedReplicateRequest.QUORUM_TYPE.ALL;
  }

  @Override
  public String getName() {
    return "drop_database";
  }

  @Override
  public int getFactoryId() {
    return FACTORYID;
  }

  @Override
  public boolean isUsingDatabase() {
    return true;
  }
}
