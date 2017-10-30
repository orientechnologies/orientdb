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

import com.orientechnologies.orient.server.distributed.ORemoteTaskFactory;
import com.orientechnologies.orient.server.distributed.task.ORemoteTask;

/**
 * Factory of remote tasks.
 *
 * @author Luca Garulli (l.garulli--at--orientdb.com)
 */
public class ODefaultRemoteTaskFactoryV0 implements ORemoteTaskFactory {
  @Override
  public ORemoteTask createTask(final int code) {
    switch (code) {
    case OCreateRecordTask.FACTORYID: // 0
      return new OCreateRecordTask();

    case OReadRecordTask.FACTORYID: // 1
      return new OReadRecordTask();

    case OReadRecordIfNotLatestTask.FACTORYID: // 2
      return new OReadRecordIfNotLatestTask();

    case OUpdateRecordTask.FACTORYID: // 3
      return new OUpdateRecordTask();

    case ODeleteRecordTask.FACTORYID: // 4
      return new ODeleteRecordTask();

    case OSQLCommandTask.FACTORYID: // 5
      return new OSQLCommandTask();

    case OScriptTask.FACTORYID: // 6
      return new OScriptTask();

    case OTxTask.FACTORYID: // 7
      return new OTxTask();

    case OCompleted2pcTask.FACTORYID: // 8
      return new OCompleted2pcTask();

    case OStopServerTask.FACTORYID: // 9
      return new OStopServerTask();

    case ORestartServerTask.FACTORYID: // 10
      return new ORestartServerTask();

    case OResurrectRecordTask.FACTORYID: // 11
      return new OResurrectRecordTask();

    case OSyncClusterTask.FACTORYID: // 12
      return new OSyncClusterTask();

    case OSyncDatabaseDeltaTask.FACTORYID: // 13
      return new OSyncDatabaseDeltaTask();

    case OSyncDatabaseTask.FACTORYID: // 14
      return new OSyncDatabaseTask();

    case OCopyDatabaseChunkTask.FACTORYID: // 15
      return new OCopyDatabaseChunkTask();

    case OGossipTask.FACTORYID: // 16
      return new OGossipTask();

    case ORepairRecordsTask.FACTORYID: // 17
      return new ORepairRecordsTask();

    case ORepairClusterTask.FACTORYID: // 18
      return new ORepairClusterTask();

    case OClusterRepairInfoTask.FACTORYID: // 19
      return new OClusterRepairInfoTask();

    case OFixCreateRecordTask.FACTORYID: // 20
      return new OFixCreateRecordTask();

    case OFixUpdateRecordTask.FACTORYID: // 21
      return new OFixUpdateRecordTask();

    case OStartReplicationTask.FACTORYID: // 22
      return new OStartReplicationTask();

    case ODropDatabaseTask.FACTORYID: // 23
      return new ODropDatabaseTask();

    case OUpdateDatabaseConfigurationTask.FACTORYID: // 24
      return new OUpdateDatabaseConfigurationTask();

    case OUpdateDatabaseStatusTask.FACTORYID: // 25
      return new OUpdateDatabaseStatusTask();

    case ODistributedLockTask.FACTORYID: // 26
      return new ODistributedLockTask();

    case ORequestDatabaseConfigurationTask.FACTORYID: // 27
      return new ORequestDatabaseConfigurationTask();

    case OUnreachableServerLocalTask.FACTORYID: // 28
      throw new IllegalArgumentException("Task with code " + code + " is not supported in remote configuration");

    }

    throw new IllegalArgumentException("Task with code " + code + " is not supported");
  }

  @Override
  public int getProtocolVersion() {
    return 0;
  }
}
