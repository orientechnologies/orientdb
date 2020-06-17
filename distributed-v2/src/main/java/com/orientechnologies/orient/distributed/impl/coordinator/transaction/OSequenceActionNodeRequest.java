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

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.client.remote.message.sequence.OSequenceActionRequest;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.config.ONodeIdentity;
import com.orientechnologies.orient.core.exception.ODatabaseException;
import com.orientechnologies.orient.core.metadata.sequence.OSequence;
import com.orientechnologies.orient.core.metadata.sequence.OSequenceAction;
import com.orientechnologies.orient.core.metadata.sequence.OSequenceCached;
import com.orientechnologies.orient.core.metadata.sequence.OSequenceHelper;
import com.orientechnologies.orient.core.metadata.sequence.OSequenceLibrary;
import com.orientechnologies.orient.core.metadata.sequence.OSequenceLimitReachedException;
import com.orientechnologies.orient.distributed.impl.ODatabaseDocumentDistributed;
import com.orientechnologies.orient.distributed.impl.coordinator.OCoordinateMessagesFactory;
import com.orientechnologies.orient.distributed.impl.coordinator.ODistributedExecutor;
import com.orientechnologies.orient.distributed.impl.coordinator.ONodeRequest;
import com.orientechnologies.orient.distributed.impl.coordinator.ONodeResponse;
import com.orientechnologies.orient.distributed.impl.log.OLogId;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

/** @author marko */
public class OSequenceActionNodeRequest implements ONodeRequest {

  private OSequenceActionRequest actionRequest;
  private ONodeIdentity initialNodeName;

  public OSequenceActionNodeRequest() {}

  public OSequenceActionNodeRequest(OSequenceActionRequest action, ONodeIdentity initialNodeName) {
    actionRequest = action;
    this.initialNodeName = initialNodeName;
  }

  @Override
  public ONodeResponse execute(
      ONodeIdentity nodeFrom,
      OLogId opId,
      ODistributedExecutor executor,
      ODatabaseDocumentInternal session) {
    int actionType = -1;
    try {
      OSequenceAction action = actionRequest.getAction();
      ODatabaseDocumentDistributed db = (ODatabaseDocumentDistributed) session;
      String localName = db.getLocalNodeName();
      OSequenceLibrary sequences = db.getMetadata().getSequenceLibrary();
      String sequenceName = action.getSequenceName();
      actionType = action.getActionType();
      OSequence targetSequence = null;
      if (actionType != OSequenceAction.CREATE) {
        targetSequence = sequences.getSequence(sequenceName);
        if (targetSequence == null) {
          throw new ODatabaseException("Sequence with name: " + sequenceName + " doesn't exists");
        }
      }
      Object result = null;
      switch (actionType) {
        case OSequenceAction.CREATE:
          OSequence sequence =
              OSequenceHelper.createSequenceOnLocal(
                  sequences, sequenceName, action.getSequenceType(), action.getParameters());
          if (sequence == null) {
            throw new ODatabaseException("Faled to create sequence: " + sequenceName);
          }
          result = sequence.getName();
          break;
        case OSequenceAction.REMOVE:
          OSequenceHelper.dropLocalSequence(sequences, sequenceName);
          result = sequenceName;
          break;
        case OSequenceAction.CURRENT:
          result = OSequenceHelper.sequenceCurrentOnLocal(targetSequence);
          break;
        case OSequenceAction.NEXT:
          result = OSequenceHelper.sequenceNextOnLocal(targetSequence);
          break;
        case OSequenceAction.RESET:
          result = OSequenceHelper.resetSequenceOnLocal(targetSequence);
          break;
        case OSequenceAction.UPDATE:
          result = OSequenceHelper.updateParamsOnLocal(action.getParameters(), targetSequence);
          break;
        case OSequenceAction.SET_NEXT:
          if (targetSequence != null
              && targetSequence.getSequenceType() == OSequence.SEQUENCE_TYPE.CACHED) {
            OSequenceCached cached = (OSequenceCached) targetSequence;
            result =
                OSequenceHelper.sequenceNextWithNewCurrentValueOnLocal(
                    cached, action.getCurrentValue());
          } else {
            throw new RuntimeException("Action SET_NEXT is reserved only for CACHED sequences");
          }
          break;
      }
      // want to return result only from node that initiated whole action
      // know how to process this in Handler
      if (!Objects.equals(localName, initialNodeName)) {
        System.out.println("EXECUTE ON OTHER NODE THAN SENDER");
        result = null;
      }
      return new OSequenceActionNodeResponse(
          OSequenceActionNodeResponse.Type.SUCCESS, null, result);
    } catch (OSequenceLimitReachedException e) {
      return new OSequenceActionNodeResponse(
          OSequenceActionNodeResponse.Type.LIMIT_REACHED, null, null);
    } catch (RuntimeException exc) {
      OLogManager.instance()
          .error(this, "Can not execute sequence action: " + actionType, exc, (Object) null);
      return new OSequenceActionNodeResponse(
          OSequenceActionNodeResponse.Type.ERROR, exc.getMessage(), null);
    }
  }

  @Override
  public void serialize(DataOutput output) throws IOException {
    if (actionRequest != null) {
      output.writeByte(1);
      actionRequest.serialize(output);
    } else {
      output.write(0);
    }
    if (initialNodeName != null) {
      output.writeByte(1);
      initialNodeName.serialize(output);
    } else {
      output.writeByte(0);
    }
  }

  @Override
  public void deserialize(DataInput input) throws IOException {
    byte flag = input.readByte();
    if (flag > 0) {
      actionRequest = new OSequenceActionRequest();
      actionRequest.deserialize(input);
    } else {
      actionRequest = null;
    }
    flag = input.readByte();
    if (flag > 0) {
      initialNodeName = new ONodeIdentity();
      initialNodeName.deserialize(input);
    } else {
      initialNodeName = null;
    }
  }

  @Override
  public int getRequestType() {
    return OCoordinateMessagesFactory.SEQUENCE_ACTION_NODE_REQUEST;
  }
}
