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

import com.orientechnologies.orient.server.distributed.ORemoteTaskFactory;

/**
 * Factory of remote tasks.
 *
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 */
public class ODefaultRemoteTaskFactory implements ORemoteTaskFactory {
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

    case OCompletedTxTask.FACTORYID: // 8
      return new OCompletedTxTask();

    case OStopNodeTask.FACTORYID: // 9
      return new OStopNodeTask();

    case ORestartNodeTask.FACTORYID: // 10
      return new ORestartNodeTask();

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
    }

    throw new IllegalArgumentException("Task with code " + code + " is not supported");
  }
}
