/*
 *
 *  *  Copyright 2016 Orient Technologies LTD (info(at)orientdb.com)
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

import com.orientechnologies.orient.core.record.impl.ODocument;
import java.util.Set;

/** @author Luca Garulli (l.garulli--at--orientdb.com) */
public interface ODistributedMessageService {
  ODistributedDatabase registerDatabase(String iDatabaseName);

  Set<String> getDatabases();

  ODistributedDatabase getDatabase(String iDatabaseName);

  ODistributedDatabase unregisterDatabase(String iDatabaseName);

  void dispatchResponseToThread(final ODistributedResponse response);

  void updateLatency(String metricName, long sentOn);

  ODocument getLatencies();

  ODocument getMessageStats();

  void updateMessageStats(String message);

  long getReceivedRequests();

  long getProcessedRequests();

  long getCurrentLatency(String server);

  ODistributedResponseManager getResponseManager(ODistributedRequestId reqId);

  void registerRequest(final long id, final ODistributedResponseManager currentResponseMgr);
}
