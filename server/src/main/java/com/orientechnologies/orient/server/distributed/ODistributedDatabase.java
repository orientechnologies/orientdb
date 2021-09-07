/*
 *
 *  *  Copyright 2016 Orient Technologies LTD (info(at)orientechnologies.com)
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
 *  * For more information: http://www.orientdb.com
 *
 */
package com.orientechnologies.orient.server.distributed;

import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.tx.OTransactionId;
import com.orientechnologies.orient.core.tx.OTransactionSequenceStatus;
import com.orientechnologies.orient.core.tx.OTxMetadataHolder;
import com.orientechnologies.orient.core.tx.ValidationResult;
import com.orientechnologies.orient.server.distributed.task.ORemoteTask;
import java.util.List;
import java.util.Optional;

/**
 * Generic Distributed Database interface.
 *
 * @author Luca Garulli (l.garulli--at--orientdb.com)
 */
public interface ODistributedDatabase {

  String getDatabaseName();

  void setOnline();

  String dump();

  void unlockResourcesOfServer(ODatabaseDocumentInternal database, String serverName);

  /**
   * Unlocks all the record locked by node iNodeName
   *
   * @param nodeName node id
   */
  void handleUnreachableNode(String nodeName);

  void waitForOnline();

  void reEnqueue(
      final int senderNodeId,
      final long msgSequence,
      final String databaseName,
      final ORemoteTask payload,
      int retryCount,
      int autoRetryDelay);

  void processRequest(ODistributedRequest request, boolean waitForAcceptingRequests);

  ValidationResult validate(OTransactionId id);

  Optional<OTransactionSequenceStatus> status();

  void rollback(OTransactionId id);

  OTxMetadataHolder commit(OTransactionId id);

  ODistributedTxContext registerTxContext(
      final ODistributedRequestId reqId, ODistributedTxContext ctx);

  ODistributedTxContext popTxContext(ODistributedRequestId requestId);

  ODistributedTxContext getTxContext(ODistributedRequestId requestId);

  ODistributedServerManager getManager();

  ODatabaseDocumentInternal getDatabaseInstance();

  long getReceivedRequests();

  long getProcessedRequests();

  void checkNodeInConfiguration(String serverName);

  Optional<OTransactionId> nextId();

  List<OTransactionId> missingTransactions(OTransactionSequenceStatus lastState);

  void validateStatus(OTransactionSequenceStatus status);

  void checkReverseSync(OTransactionSequenceStatus lastState);

  ODistributedConfiguration getDistributedConfiguration();

  void setDistributedConfiguration(
      final OModifiableDistributedConfiguration distributedConfiguration);

  void fillStatus();
}
