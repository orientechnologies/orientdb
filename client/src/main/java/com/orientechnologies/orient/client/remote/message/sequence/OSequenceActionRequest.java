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
package com.orientechnologies.orient.client.remote.message.sequence;

import com.orientechnologies.orient.core.metadata.sequence.OSequence;
import com.orientechnologies.orient.core.metadata.sequence.OSequenceAction;
import com.orientechnologies.orient.core.metadata.sequence.SequenceOrderType;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

/** @author marko */
public class OSequenceActionRequest {

  private OSequenceAction action = null;

  public OSequenceActionRequest() {}

  public OSequenceActionRequest(OSequenceAction action) {
    this.action = action;
  }

  private void serializeInt(Integer val, DataOutput out) throws IOException {
    if (val != null) {
      // determines that value is not null
      out.writeByte(1);
      out.writeInt(val);
    } else {
      // determines that value is null
      out.writeByte(0);
    }
  }

  private Integer deserializeInt(DataInput in) throws IOException {
    Integer retval = null;
    byte nullVal = in.readByte();
    if (nullVal > 0) {
      retval = in.readInt();
    }
    return retval;
  }

  private void serializeLong(Long val, DataOutput out) throws IOException {
    if (val != null) {
      // determines that value is not null
      out.writeByte(1);
      out.writeLong(val);
    } else {
      // determines that value is null
      out.writeByte(0);
    }
  }

  private Long deserializeLong(DataInput in) throws IOException {
    Long retval = null;
    byte nullVal = in.readByte();
    if (nullVal > 0) {
      retval = in.readLong();
    }
    return retval;
  }

  private void serializeOrderType(SequenceOrderType val, DataOutput out) throws IOException {
    if (val != null) {
      // determines that value is not null
      out.writeByte(1);
      out.writeByte(val.getValue());
    } else {
      // determines that value is null
      out.writeByte(0);
    }
  }

  private SequenceOrderType deserializeOrderType(DataInput in) throws IOException {
    SequenceOrderType retval = null;
    byte nullVal = in.readByte();
    if (nullVal > 0) {
      retval = SequenceOrderType.fromValue(in.readByte());
    }
    return retval;
  }

  private void serializeBoolean(Boolean val, DataOutput out) throws IOException {
    if (val != null) {
      // determines that value is not null
      out.writeByte(1);
      out.writeBoolean(val);
    } else {
      // determines that value is null
      out.writeByte(0);
    }
  }

  private Boolean deserializeBoolean(DataInput in) throws IOException {
    Boolean retval = null;
    byte nullVal = in.readByte();
    if (nullVal > 0) {
      retval = in.readBoolean();
    }
    return retval;
  }

  public void serialize(DataOutput out) throws IOException {
    if (action != null) {
      out.writeInt(action.getActionType());
      byte[] sequenceNameBytes = action.getSequenceName().getBytes(StandardCharsets.UTF_8.name());
      out.writeInt(sequenceNameBytes.length);
      out.write(sequenceNameBytes);
      serializeLong(action.getCurrentValue(), out);
      out.writeByte(action.getSequenceType().getVal());

      OSequence.CreateParams params = action.getParameters();
      if (params == null) {
        out.writeByte(0);
      } else {
        out.writeByte(1);
        serializeLong(params.getStart(), out);
        serializeInt(params.getIncrement(), out);
        serializeInt(params.getCacheSize(), out);
        serializeLong(params.getLimitValue(), out);
        serializeOrderType(params.getOrderType(), out);
        serializeBoolean(params.getRecyclable(), out);
        serializeBoolean(params.getTurnLimitOff(), out);
        serializeLong(params.getCurrentValue(), out);
      }
    } else {
      out.writeInt(-1);
    }
  }

  public void deserialize(DataInput in) throws IOException {
    int actionType = in.readInt();
    if (actionType > 0) {
      int nameLength = in.readInt();
      byte[] nameBytes = new byte[nameLength];
      in.readFully(nameBytes);
      String sequenceName = new String(nameBytes, StandardCharsets.UTF_8.name());
      Long currentValue = deserializeLong(in);
      byte sequenceTypeByte = in.readByte();
      OSequence.SEQUENCE_TYPE sequenceType = OSequence.SEQUENCE_TYPE.fromVal(sequenceTypeByte);
      if (sequenceType == null) {
        throw new IOException("Inavlid sequnce type value: " + sequenceTypeByte);
      }

      OSequence.CreateParams params = null;
      byte paramsNullFlag = in.readByte();
      if (paramsNullFlag > 0) {
        params = new OSequence.CreateParams();
        params.resetNull();
        params.setStart(deserializeLong(in));
        params.setIncrement(deserializeInt(in));
        params.setCacheSize(deserializeInt(in));
        params.setLimitValue(deserializeLong(in));
        params.setOrderType(deserializeOrderType(in));
        params.setRecyclable(deserializeBoolean(in));
        params.setTurnLimitOff(deserializeBoolean(in));
        params.setCurrentValue(deserializeLong(in));
      }
      if (currentValue != null) {
        action = new OSequenceAction(sequenceName, currentValue);
      } else {
        action = new OSequenceAction(actionType, sequenceName, params, sequenceType);
      }
    } else {
      action = null;
    }
  }

  public OSequenceAction getAction() {
    return action;
  }
}
