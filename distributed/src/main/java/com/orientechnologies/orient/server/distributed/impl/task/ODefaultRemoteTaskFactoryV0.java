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

    case OSQLCommandTask.FACTORYID: // 5
      return new OSQLCommandTask();

    case OScriptTask.FACTORYID: // 6
      return new OScriptTask();

    case OStopServerTask.FACTORYID: // 9
      return new OStopServerTask();

    case ORestartServerTask.FACTORYID: // 10
      return new ORestartServerTask();

    case OSyncClusterTask.FACTORYID: // 12
      return new OSyncClusterTask();

    case OSyncDatabaseTask.FACTORYID: // 14
      return new OSyncDatabaseTask();

    case OCopyDatabaseChunkTask.FACTORYID: // 15
      return new OCopyDatabaseChunkTask();

    case OGossipTask.FACTORYID: // 16
      return new OGossipTask();

    case ODropDatabaseTask.FACTORYID: // 23
      return new ODropDatabaseTask();

    case OUpdateDatabaseConfigurationTask.FACTORYID: // 24
      return new OUpdateDatabaseConfigurationTask();

    case OUpdateDatabaseStatusTask.FACTORYID: // 25
      return new OUpdateDatabaseStatusTask();

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
