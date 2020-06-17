/*
 * Copyright 2018 OrientDB.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.orientechnologies.orient.distributed.impl.coordinator.transaction;

import com.orientechnologies.orient.core.db.config.ONodeIdentity;
import com.orientechnologies.orient.distributed.impl.coordinator.ODistributedCoordinator;
import com.orientechnologies.orient.distributed.impl.coordinator.ONodeResponse;
import com.orientechnologies.orient.distributed.impl.coordinator.ORequestContext;
import com.orientechnologies.orient.distributed.impl.coordinator.OResponseHandler;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/** @author marko */
public class OSequenceActionNodeResponseHandler implements OResponseHandler {

  private int responseCount = 0;
  private final List<ONodeIdentity> failedActionNode = new ArrayList<>();
  private final List<ONodeIdentity> limitReachedNodes = new ArrayList<>();
  private final OSessionOperationId operationId;
  private Object senderResult = null;
  private final ONodeIdentity initiator;

  //  private OSequenceActionNodeResponseHandler(){
  //
  //  }

  public OSequenceActionNodeResponseHandler(
      OSessionOperationId operationId, ONodeIdentity initiator) {
    this.operationId = operationId;
    this.initiator = initiator;
  }

  @Override
  public boolean receive(
      ODistributedCoordinator coordinator,
      ORequestContext context,
      ONodeIdentity member,
      ONodeResponse response) {
    responseCount++;
    OSequenceActionNodeResponse responseFromNode = (OSequenceActionNodeResponse) response;
    switch (responseFromNode.getResponseResultType()) {
      case SUCCESS:
        // sender result is different from null only from node that initialized whole process
        if (senderResult == null) {
          senderResult = responseFromNode.getResponseResult();
        }
        break;
      case ERROR:
        failedActionNode.add(member);
        break;
      case LIMIT_REACHED:
        limitReachedNodes.add(member);
        break;
    }
    if (responseCount == context.getInvolvedMembers().size()) {
      List<ONodeIdentity> failedNodesNames = failedActionNode.stream().collect(Collectors.toList());
      List<ONodeIdentity> limitReachedNodeNames =
          limitReachedNodes.stream().collect(Collectors.toList());
      OSequenceActionCoordinatorResponse submitedActionResponse =
          new OSequenceActionCoordinatorResponse(
              failedNodesNames,
              limitReachedNodeNames,
              senderResult,
              context.getInvolvedMembers().size());
      coordinator.reply(initiator, operationId, submitedActionResponse);
      return true;
    }
    return false;
  }

  @Override
  public boolean timeout(ODistributedCoordinator coordinator, ORequestContext context) {
    // TODO think about timeouts
    return false;
  }
}
