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
package com.orientechnologies.orient.server.distributed.impl.coordinator.transaction;

import com.orientechnologies.orient.client.remote.message.sequence.OSequenceActionRequest;
import com.orientechnologies.orient.core.metadata.sequence.OSequenceAction;
import com.orientechnologies.orient.server.distributed.impl.coordinator.OCoordinateMessagesFactory;
import com.orientechnologies.orient.server.distributed.impl.coordinator.ODistributedCoordinator;
import com.orientechnologies.orient.server.distributed.impl.coordinator.ODistributedMember;
import com.orientechnologies.orient.server.distributed.impl.coordinator.OSubmitRequest;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 *
 * @author marko
 */
public class OSequenceActionsSubmit implements OSubmitRequest{  
  
  private List<OSequenceActionRequest> actions = null;
  
  public OSequenceActionsSubmit(){
    
  }
  
  public OSequenceActionsSubmit(List<OSequenceAction> actions){
    this.actions = new ArrayList<>();
    for (OSequenceAction action : actions){
      OSequenceActionRequest actionrequest = new OSequenceActionRequest(action);
      this.actions.add(actionrequest);
    }
  }
  
  @Override
  public void begin(ODistributedMember member, OSessionOperationId operationId, ODistributedCoordinator coordinator) {
    OSequenceActionsNodeRequest nodeRequest = new OSequenceActionsNodeRequest();
    OSequenceActionsNodeResponseHandler nodeResponseHandler = new OSequenceActionsNodeResponseHandler();
    
    coordinator.sendOperation(this, nodeRequest, nodeResponseHandler);
  }

  @Override
  public void serialize(DataOutput output) throws IOException {
    output.writeInt(actions.size());
    for (OSequenceActionRequest actionsRequest : actions){
      actionsRequest.serialize(output);
    }
  }

  @Override
  public void deserialize(DataInput input) throws IOException {
    this.actions = new ArrayList<>();
    int size = input.readInt();
    for (int i = 0; i < size; i++){
      OSequenceActionRequest actionrequest = new OSequenceActionRequest();
      actionrequest.deserialize(input);
      actions.add(actionrequest);
    }
  }

  @Override
  public int getRequestType() {
    return OCoordinateMessagesFactory.SEQUENCE_ACTIONS_SUBMIT_REQUEST;
  }
  
}
