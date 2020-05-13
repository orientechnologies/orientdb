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
package com.orientechnologies.orient.server.distributed;

import java.util.Set;

public interface ODistributedMessageListener {

  default void onMessageReceived(ODistributedRequest request) {

  }

  default void onMessagePartitionCalculated(ODistributedRequest request, Set<Integer> involvedWorkerQueues) {

  }

  default void onMessageBeforeOp(String op, ODistributedRequestId requestId) {

  }

  default void onMessageAfterOp(String op, ODistributedRequestId requestId) {

  }

  default void onMessageProcessStart(ODistributedRequest message) {

  }

  default void onMessageCurrentPayload(ODistributedRequestId requestId, Object responsePayload) {

  }

  default void onMessageProcessEnd(ODistributedRequest iRequest, Object responsePayload) {

  }

}
