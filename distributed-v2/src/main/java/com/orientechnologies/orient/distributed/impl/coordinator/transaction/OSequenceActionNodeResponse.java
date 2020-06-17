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

import com.orientechnologies.orient.distributed.impl.coordinator.OCoordinateMessagesFactory;
import com.orientechnologies.orient.distributed.impl.coordinator.ONodeResponse;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

/** @author marko */
public class OSequenceActionNodeResponse implements ONodeResponse {
  public enum Type {
    SUCCESS((byte) 1),
    ERROR((byte) 2),
    LIMIT_REACHED((byte) 3);

    private final byte val;

    Type(byte val) {
      this.val = val;
    }

    public byte getVal() {
      return val;
    }

    public static Type fromVal(byte val) {
      switch (val) {
        case 1:
          return SUCCESS;
        case 2:
          return ERROR;
        case 3:
          return LIMIT_REACHED;
        default:
          return null;
      }
    }
  };

  private Type responseResultType;
  private String message;
  private Object responseResult;

  public OSequenceActionNodeResponse() {}

  public OSequenceActionNodeResponse(Type responseType, String message, Object responseResult) {
    this.responseResultType = responseType;
    this.message = message;
    this.responseResult = responseResult;
  }

  protected static void serializeResult(DataOutput output, Object result) throws IOException {
    if (result == null) {
      output.writeByte(0);
      return;
    }
    if (result instanceof String) {
      output.writeByte(1);
      byte[] resBytes = ((String) result).getBytes(StandardCharsets.UTF_8.name());
      output.writeInt(resBytes.length);
      output.write(resBytes);
    } else if (result instanceof Integer) {
      output.writeByte(2);
      output.writeInt((Integer) result);
    } else if (result instanceof Long) {
      output.writeByte(3);
      output.writeLong((Long) result);
    } else if (result instanceof Boolean) {
      output.writeByte(4);
      output.writeBoolean((Boolean) result);
    } else {
      throw new IllegalArgumentException(
          "Result type not supported: " + result.getClass().getSimpleName());
    }
  }

  @Override
  public void serialize(DataOutput output) throws IOException {
    output.writeByte(responseResultType.getVal());
    if (message == null) {
      output.writeInt(-1);
    } else {
      byte[] messageBytes = message.getBytes(StandardCharsets.UTF_8.name());
      output.writeInt(messageBytes.length);
      output.write(messageBytes);
    }
    serializeResult(output, responseResult);
  }

  protected static Object deserializeResult(DataInput input) throws IOException {
    byte typeFlag = input.readByte();
    switch (typeFlag) {
      case 0:
        return null;
      case 1:
        int dataLength = input.readInt();
        byte[] dataBytes = new byte[dataLength];
        input.readFully(dataBytes);
        return new String(dataBytes, StandardCharsets.UTF_8.name());
      case 2:
        return input.readInt();
      case 3:
        return input.readLong();
      case 4:
        return input.readBoolean();
      default:
        throw new IllegalArgumentException("Inavlid type value");
    }
  }

  @Override
  public void deserialize(DataInput input) throws IOException {
    byte resultTypeOrd = input.readByte();
    responseResultType = Type.fromVal(resultTypeOrd);
    message = null;
    int messageBytesLength = input.readInt();
    if (messageBytesLength >= 0) {
      byte[] messageBytes = new byte[messageBytesLength];
      input.readFully(messageBytes);
      message = new String(messageBytes, StandardCharsets.UTF_8.name());
    }
    responseResult = deserializeResult(input);
  }

  public Type getResponseResultType() {
    return responseResultType;
  }

  @Override
  public int getResponseType() {
    return OCoordinateMessagesFactory.SEQUENCE_ACTION_NODE_RESPONSE;
  }

  public Object getResponseResult() {
    return responseResult;
  }
}
