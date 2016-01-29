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

package com.orientechnologies.orient.core.command;

import com.orientechnologies.orient.core.replication.OAsyncReplicationError;
import com.orientechnologies.orient.core.replication.OAsyncReplicationOk;

import java.util.Set;

/**
 * @author Andrey Lomakin (a.lomakin-at-orientechnologies.com)
 * @since 7/2/14
 */
public interface ODistributedCommand {
  Set<String> nodesToExclude();

  /**
   * Defines a callback to call in case of the asynchronous replication succeed.
   */
  ODistributedCommand onAsyncReplicationOk(OAsyncReplicationOk iCallback);

  /**
   * Defines a callback to call in case of error during the asynchronous replication.
   */
  ODistributedCommand onAsyncReplicationError(OAsyncReplicationError iCallback);
}
