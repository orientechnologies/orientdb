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

import com.orientechnologies.orient.server.distributed.impl.coordinator.OCoordinateMessagesFactory;
import com.orientechnologies.orient.server.distributed.impl.coordinator.OSubmitResponse;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 *
 * @author mdjurovi
 */
public class OSequenceActionCoordinatorResponse implements OSubmitResponse{
  
  private int failedOn = 0;
  private int limitReachedOn = 0;  
  private Object resultOfSenderNode;
  
  public OSequenceActionCoordinatorResponse(){
    
  }
  
  public OSequenceActionCoordinatorResponse(int failedOnNo, int limitReachedOnNo){
    failedOn = failedOnNo;
    limitReachedOn = limitReachedOnNo;
  }
  
  @Override
  public void serialize(DataOutput output) throws IOException {
    output.writeInt(failedOn);
    output.writeInt(limitReachedOn);
  }

  @Override
  public void deserialize(DataInput input) throws IOException {
    failedOn = input.readInt();
    limitReachedOn = input.readInt();
  }

  @Override
  public int getResponseType() {
    return OCoordinateMessagesFactory.SEQUENCE_ACTION_COORDINATOR_RESPONSE;
  }
  
  public boolean isSuccess(){
    return failedOn <= 0;
  }

  public Object getResultOfSenderNode() {
    return resultOfSenderNode;
  }

  public void setResultOfSenderNode(Object resultOfSenderNode) {
    this.resultOfSenderNode = resultOfSenderNode;
  }

  public int getFailedOn() {
    return failedOn;
  }

  public void setFailedOn(int failedOn) {
    this.failedOn = failedOn;
  }

  public int getLimitReachedOn() {
    return limitReachedOn;
  }

  public void setLimitReachedOn(int limitReachedOn) {
    this.limitReachedOn = limitReachedOn;
  }    
  
}
