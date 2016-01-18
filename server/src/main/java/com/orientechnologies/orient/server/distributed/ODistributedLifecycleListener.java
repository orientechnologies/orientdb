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
package com.orientechnologies.orient.server.distributed;

/**
 * Distributed lifecycle interface to catch event from the distributed cluster.
 *
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 *
 */
public interface ODistributedLifecycleListener {

  /**
   * Called when a node is joining the cluster. Return false to deny the join.
   * 
   * @param iNode
   *          Node name that is joining
   * @return true to allow the join, otherwise false
   */
  boolean onNodeJoining(String iNode);

  /**
   * Called right after a node joined the cluster.
   *
   * @param iNode
   *          Node name that is joining
   */
  void onNodeJoined(String iNode);

  /**
   * Called right after a node left the cluster.
   *
   * @param iNode
   *          Node name that left
   */
  void onNodeLeft(String iNode);

  /**
   * Called upon change of database status on a node. Available statuses are defined in ODistributedServerManager.DB_STATUS.
   * 
   * @since 2.2.0
   * @param iNode
   *          The node name
   * @param iDatabaseName
   *          Database name
   * @param iNewStatus
   *          The new status
   */
  void onDatabaseChangeStatus(String iNode, String iDatabaseName, ODistributedServerManager.DB_STATUS iNewStatus);
}
