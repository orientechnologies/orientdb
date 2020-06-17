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
import com.orientechnologies.orient.distributed.impl.coordinator.OCoordinateMessagesFactory;
import com.orientechnologies.orient.distributed.impl.coordinator.OSubmitResponse;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/** @author mdjurovi */
public class OSequenceActionCoordinatorResponse implements OSubmitResponse {

  private List<ONodeIdentity> failedOn;
  private List<ONodeIdentity> limitReachedOn;
  private Object resultOfSenderNode;
  private int nodesInvolved = 0;

  public OSequenceActionCoordinatorResponse() {}

  public OSequenceActionCoordinatorResponse(
      List<ONodeIdentity> failedOnNo,
      List<ONodeIdentity> limitReachedOnNo,
      Object result,
      int numOfNodes) {
    failedOn = failedOnNo;
    limitReachedOn = limitReachedOnNo;
    resultOfSenderNode = result;
    this.nodesInvolved = numOfNodes;
  }

  private static void serializeList(DataOutput output, List<ONodeIdentity> list)
      throws IOException {
    if (list == null) {
      output.writeByte(-1);
    } else {
      output.writeByte(1);
      output.writeInt(list.size());
      for (ONodeIdentity identity : list) {
        if (identity == null) {
          output.writeByte(0);
        } else {
          output.writeByte(1);
          identity.serialize(output);
        }
      }
    }
  }

  private static List<ONodeIdentity> deserializeList(DataInput input) throws IOException {
    byte nullFlag = input.readByte();
    if (nullFlag > -1) {
      int listSize = input.readInt();
      List<ONodeIdentity> retList = new ArrayList<>();
      for (int i = 0; i < listSize; i++) {
        byte stringByte = input.readByte();
        if (stringByte > 0) {
          ONodeIdentity identity = new ONodeIdentity();
          identity.deserialize(input);
          retList.add(identity);
        } else {
          retList.add(null);
        }
      }
      return retList;
    } else {
      return null;
    }
  }

  @Override
  public void serialize(DataOutput output) throws IOException {
    serializeList(output, failedOn);
    serializeList(output, limitReachedOn);
    output.writeInt(nodesInvolved);
    OSequenceActionNodeResponse.serializeResult(output, resultOfSenderNode);
  }

  @Override
  public void deserialize(DataInput input) throws IOException {
    failedOn = deserializeList(input);
    limitReachedOn = deserializeList(input);
    nodesInvolved = input.readInt();
    resultOfSenderNode = OSequenceActionNodeResponse.deserializeResult(input);
  }

  @Override
  public int getResponseType() {
    return OCoordinateMessagesFactory.SEQUENCE_ACTION_COORDINATOR_RESPONSE;
  }

  public boolean isSuccess() {
    return failedOn.size() <= 0;
  }

  public Object getResultOfSenderNode() {
    return resultOfSenderNode;
  }

  public List<ONodeIdentity> getFailedOn() {
    return failedOn;
  }

  public List<ONodeIdentity> getLimitReachedOn() {
    return limitReachedOn;
  }

  public int getNumberOfNodesInvolved() {
    return nodesInvolved;
  }
}
