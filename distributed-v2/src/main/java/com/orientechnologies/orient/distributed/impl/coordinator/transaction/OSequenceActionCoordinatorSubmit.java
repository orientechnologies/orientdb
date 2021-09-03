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

import com.orientechnologies.orient.client.remote.message.sequence.OSequenceActionRequest;
import com.orientechnologies.orient.core.db.config.ONodeIdentity;
import com.orientechnologies.orient.core.metadata.sequence.OSequenceAction;
import com.orientechnologies.orient.distributed.impl.coordinator.OCoordinateMessagesFactory;
import com.orientechnologies.orient.distributed.impl.coordinator.ODistributedCoordinator;
import com.orientechnologies.orient.distributed.impl.coordinator.OSubmitRequest;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/** @author marko */
public class OSequenceActionCoordinatorSubmit implements OSubmitRequest {

  private OSequenceActionRequest action = null;

  public OSequenceActionCoordinatorSubmit() {}

  public OSequenceActionCoordinatorSubmit(OSequenceAction action) {
    this.action = new OSequenceActionRequest(action);
  }

  @Override
  public void begin(
      ONodeIdentity requester,
      OSessionOperationId operationId,
      ODistributedCoordinator coordinator) {
    OSequenceActionNodeRequest nodeRequest = new OSequenceActionNodeRequest(action, requester);
    OSequenceActionNodeResponseHandler nodeResponseHandler =
        new OSequenceActionNodeResponseHandler(operationId, requester);

    coordinator.sendOperation(this, nodeRequest, nodeResponseHandler);
  }

  @Override
  public void serialize(DataOutput output) throws IOException {
    if (action != null) {
      output.writeByte(1);
      action.serialize(output);
    } else {
      output.writeByte(0);
    }
  }

  @Override
  public void deserialize(DataInput input) throws IOException {
    byte flag = input.readByte();
    action = null;
    if (flag > 0) {
      action = new OSequenceActionRequest();
      action.deserialize(input);
    }
  }

  @Override
  public int getRequestType() {
    return OCoordinateMessagesFactory.SEQUENCE_ACTION_COORDINATOR_SUBMIT;
  }
}
