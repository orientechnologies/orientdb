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

/**
 * Interface to know if the command must be distributed in clustered scenario.
 * 
 * @author Luca Garulli
 */
public interface OCommandDistributedReplicateRequest {

  enum DISTRIBUTED_EXECUTION_MODE {
    LOCAL, REPLICATE
  }

  enum DISTRIBUTED_RESULT_MGMT {
    CHECK_FOR_EQUALS, MERGE
  }

  enum QUORUM_TYPE {
    NONE, READ, WRITE, ALL
  }

  /**
   * Returns the execution mode when distributed configuration is active:
   * <ul>
   * <li>LOCAL: executed on local node only</li>
   * <li>REPLICATE: executed on all the nodes and expect the same result</li>
   * <li>SHARDED: executed on all the involved nodes and merge results</li>
   * </ul>
   */
  DISTRIBUTED_EXECUTION_MODE getDistributedExecutionMode();

  /**
   * Returns how to manage the distributed result between:
   * <ul>
   * <li>CHECK_FOR_EQUALS: all results must be the same</li>
   * <li>MERGE: merges results. This is typically used on sharding</li>
   * </ul>
   */
  DISTRIBUTED_RESULT_MGMT getDistributedResultManagement();

  /**
   * Returns the quorum type for the command:
   * <ul>
   * <li>NONE: no quorum</li>
   * <li>READ: configured Read quorum</li>
   * <li>WRITE: configured Write quorum</li>
   * <li>ALL: all nodes</li>
   * </ul>
   */
  QUORUM_TYPE getQuorumType();

  /**
   * Returns the distributed timeout in milliseconds.
   */
  long getDistributedTimeout();
}
