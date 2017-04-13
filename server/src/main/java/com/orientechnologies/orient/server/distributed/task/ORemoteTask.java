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
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.distributed.ODistributedRequestId;
import com.orientechnologies.orient.server.distributed.ODistributedServerManager;
import com.orientechnologies.orient.server.distributed.ORemoteTaskFactory;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Remote Task interface.
 *
 * @author Luca Garulli
 */
public interface ORemoteTask {
  boolean hasResponse();

  enum RESULT_STRATEGY {
    ANY, UNION
  }

  String getName();

  OCommandDistributedReplicateRequest.QUORUM_TYPE getQuorumType();

  Object execute(ODistributedRequestId requestId, OServer iServer, ODistributedServerManager iManager,
      ODatabaseDocumentInternal database) throws Exception;

  int[] getPartitionKey();

  long getDistributedTimeout();

  long getSynchronousTimeout(final int iSynchNodes);

  long getTotalTimeout(final int iTotalNodes);

  OAbstractRemoteTask.RESULT_STRATEGY getResultStrategy();

  String getNodeSource();

  void setNodeSource(String nodeSource);

  boolean isIdempotent();

  boolean isNodeOnlineRequired();

  boolean isUsingDatabase();

  int getFactoryId();

  /**
   * Checks the request is still valid.
   */
  void checkIsValid(ODistributedServerManager dManager);

  void toStream(DataOutput out) throws IOException;

  void fromStream(DataInput in, ORemoteTaskFactory factory) throws IOException;
}