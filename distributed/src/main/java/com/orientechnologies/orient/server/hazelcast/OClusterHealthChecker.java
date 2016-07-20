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
package com.orientechnologies.orient.server.hazelcast;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.server.distributed.ODistributedServerManager;

import java.util.TimerTask;

import static com.orientechnologies.orient.server.hazelcast.OHazelcastPlugin.CONFIG_DBSTATUS_PREFIX;

/**
 * Timer task that checks periodically the cluster health status.
 *
 * @author Luca Garulli (l.garulli--at--orientdb.com)
 */
public class OClusterHealthChecker extends TimerTask {
  private final ODistributedServerManager manager;

  public OClusterHealthChecker(final ODistributedServerManager manager) {
    this.manager = manager;
  }

  public void run() {
    // CHECK CURRENT STATUS OF DBS
    OLogManager.instance().debug(this, "Checking cluster health...");
    try {

      checkDatabaseStatuses();

    } catch (Throwable t) {
      OLogManager.instance().error(this, "Error on checking cluster health", t);
    } finally {
      OLogManager.instance().debug(this, "Cluster health checking completed");
    }
  }

  protected void checkDatabaseStatuses() {
    for (String dbName : manager.getMessageService().getDatabases()) {
      final ODistributedServerManager.DB_STATUS status = (ODistributedServerManager.DB_STATUS) manager.getConfigurationMap()
          .get(CONFIG_DBSTATUS_PREFIX + manager.getLocalNodeName() + "." + dbName);
      if (status == null) {
        OLogManager.instance().warn(this, "Status of database '%s' on server '%s' is missing", dbName,
            manager.getLocalNodeName());
      }
    }
  }
}
