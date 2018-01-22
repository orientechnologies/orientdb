package com.orientechnologies.agent.cloud.processor.tasks;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.orientechnologies.orient.server.distributed.operation.NodeOperationResponse;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by Enrico Risa on 17/01/2018.
 */
public abstract class AbstractRPCTaskResponse<T> implements NodeOperationResponse {

  private T payload;
  ObjectMapper mapper = new ObjectMapper();

  public AbstractRPCTaskResponse() {
  }

  public AbstractRPCTaskResponse(T payload) {
    this.payload = payload;
  }

  public T getPayload() {
    return payload;
  }

  protected abstract Class<T> getPayloadType();

  @Override
  public void write(DataOutput out) throws IOException {

    String msg = mapper.writeValueAsString(getPayload());
    byte[] message = msg.getBytes("UTF-8");
    out.writeInt(message.length);
    out.write(message);

  }

  @Override
  public void read(DataInput in) throws IOException {

    int size = in.readInt();
    byte[] message = new byte[size];
    in.readFully(message, 0, size);
    String msg = new String(message, "UTF-8");
    payload = mapper.readValue(msg, getPayloadType());
  }
}
